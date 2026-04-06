"""Company profiles exporter.

Replaces: Supabase edge functions api (pageBuilder) + company-basic

Output:
- companies/{cvr}/basic.json (lightweight company info for hero section)
- companies/{cvr}.json (full company profile with pageBuilder components)

Data sources (R2):
- gold/cvr_enrichment_companies/data.parquet — company identity (61k)
- gold/cvr_enrichment_financial_statements/20260323_043837/financial_statements.parquet
- gold/field_production/latest/data.parquet — field/crop data (10.8M rows)
- gold/pesticide_disaggregation_2023_2024/20260317_074432/...parquet
- gold/worker_safety/20260317_074628/worker_safety_clean.parquet
- gold/work_permits/20260317_074636/work_permits.parquet
- gold/field_environmental_analysis_fields_2024/data.parquet — BNBO/wetlands/grukos (618k)
- gold/nles5_nitrogen_estimation_nitrogen_estimates/data.parquet — nitrogen leaching (577k)
- silver/arbejdstilsynet_inspections/20260323_145025/workplace_inspections.parquet (545)
- silver/subsidies/20260322_074339/stoetteoplysninger...parquet — EU subsidy payments
- silver/subsidies/20260322_074339/Landbrugsstoette_2023.parquet — 2023 subsidies
"""

import logging
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from exporters.base import BaseExporter

logger = logging.getLogger("api_export.company_profiles")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


def _r2(path: str) -> str:
    return f"r2://{BUCKET}/{path}"


def _rclone_to_local(r2_path: str, local_path: str) -> str:
    """Download a file from R2 via rclone (handles spaces in filenames)."""
    result = subprocess.run(
        ["rclone", "copyto", f"r2:{BUCKET}/{r2_path}", local_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise OSError(f"rclone failed for {r2_path}: {result.stderr}")
    return local_path


class CompanyProfilesExporter(BaseExporter):
    """Generate per-company JSON files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tmp_dir = tempfile.mkdtemp(prefix="api_export_")

    def export(self) -> dict:
        stats = {"basic_files": 0, "full_files": 0, "files_written": 0}

        self._load_tables()

        # companies table is required — skip entire exporter if missing
        available = {
            r[0]
            for r in self.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "companies" not in available:
            logger.warning("Companies table not loaded — skipping company profiles exporter")
            return stats

        self._precompute_aggregates()

        stats["basic_files"] = self._generate_basic_files()
        stats["files_written"] += stats["basic_files"]

        stats["full_files"] = self._generate_full_profiles()
        stats["files_written"] += stats["full_files"]

        return stats

    def _load_tables(self) -> None:
        """Load all data sources into DuckDB tables."""
        # Direct R2 paths (no spaces)
        r2_tables = {
            "companies": _r2("gold/cvr_enrichment_companies/data.parquet"),
            "financials": _r2(
                "gold/cvr_enrichment_financial_statements/20260323_043837/financial_statements.parquet"
            ),
            "field_production": _r2("gold/field_production/latest/data.parquet"),
            "pesticides": _r2(
                "gold/pesticide_disaggregation_2023_2024/20260317_074432/pesticide_disaggregation_2023_2024.parquet"
            ),
            "worker_safety": _r2("gold/worker_safety/20260317_074628/worker_safety_clean.parquet"),
            "work_permits": _r2("gold/work_permits/20260317_074636/work_permits.parquet"),
            "env_fields": _r2("gold/field_environmental_analysis_fields_2024/data.parquet"),
            "nitrogen": _r2("gold/nles5_nitrogen_estimation_nitrogen_estimates/data.parquet"),
        }
        for name, path in r2_tables.items():
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(f"Could not load {name} from {path}")

        # Files with spaces in name — download via rclone first
        rclone_tables = {
            "inspections": "silver/arbejdstilsynet_inspections/20260323_145025/workplace_inspections.parquet",
            "subsidies_eu": "silver/subsidies/20260322_074339/stoetteoplysninger.naturerhverv.dk_20241223_pii_handled.parquet",
            "subsidies_2023": "silver/subsidies/20260322_074339/Landbrugsstoette_2023.parquet",
            # fertiliser data has NULL n_kvote — not usable yet
        }
        for name, r2_path in rclone_tables.items():
            local = str(Path(self._tmp_dir) / f"{name}.parquet")
            try:
                _rclone_to_local(r2_path, local)
                self.load_parquet_table(local, name)
            except Exception:
                logger.warning(f"Could not load {name} via rclone from {r2_path}")

    def _precompute_aggregates(self) -> None:
        """Pre-compute per-company aggregates to avoid N+1 queries."""
        logger.info("Pre-computing per-company aggregates...")

        self._precompute_field_production()
        self._precompute_financials()
        self._precompute_pesticides()
        self._precompute_worker_safety()
        self._precompute_work_permits()

        # Environmental analysis per company (BNBO, wetlands, grukos)
        self._precompute_env()

        # Nitrogen leaching per company
        self._precompute_nitrogen()

        # Workplace inspections per company
        self._precompute_inspections()

        # Subsidies per company
        self._precompute_subsidies()

        # Fertiliser per company
        self._precompute_fertiliser()

        logger.info("Pre-computation complete")

    def _precompute_field_production(self) -> None:
        """Field production summary, yearly totals, and crop distribution."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_field_summary AS
                SELECT
                    cvr_number,
                    MAX(year) AS latest_year,
                    COUNT(DISTINCT year) AS years_active,
                    ROUND(SUM(CASE WHEN year = 2024 THEN area_ha ELSE 0 END), 1) AS area_2024_ha,
                    COUNT(CASE WHEN year = 2024 THEN 1 END) AS fields_2024,
                    COUNT(DISTINCT CASE WHEN year = 2024 THEN crop_type END) AS crops_2024,
                    ROUND(100.0 * SUM(CASE WHEN year = 2024 AND organic_farming THEN area_ha ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN year = 2024 THEN area_ha ELSE 0 END), 0), 1) AS organic_pct_2024
                FROM field_production
                GROUP BY cvr_number
            """)
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_field_yearly AS
                SELECT
                    cvr_number,
                    year,
                    ROUND(SUM(area_ha), 1) AS total_area_ha,
                    COUNT(*) AS field_count,
                    ROUND(SUM(production_estimate_hkg), 0) AS total_production_hkg
                FROM field_production
                GROUP BY cvr_number, year
                ORDER BY cvr_number, year
            """)
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_crops_2024 AS
                SELECT
                    cvr_number,
                    crop_type,
                    organic_farming,
                    ROUND(SUM(area_ha), 2) AS area_ha,
                    COUNT(*) AS field_count
                FROM field_production
                WHERE year = 2024
                GROUP BY cvr_number, crop_type, organic_farming
                ORDER BY cvr_number, area_ha DESC
            """)
        except Exception:
            logger.warning("Could not compute field production aggregates")

    def _precompute_financials(self) -> None:
        """Financial summary — latest report per company."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_financials AS
                SELECT DISTINCT ON (cvr_number)
                    cvr_number,
                    net_profit_loss,
                    gross_profit_loss,
                    total_assets,
                    total_equity,
                    equity_ratio,
                    return_on_assets,
                    average_number_of_employees,
                    property_plant_equipment,
                    reporting_period_end
                FROM financials
                ORDER BY cvr_number, reporting_period_end DESC
            """)
        except Exception:
            logger.warning("Could not compute financial aggregates")

    def _precompute_pesticides(self) -> None:
        """Pesticide summary per company."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_pesticides AS
                SELECT
                    cvr_number,
                    COUNT(*) AS total_applications,
                    COUNT(DISTINCT PesticideName) AS unique_pesticides,
                    ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                    ROUND(SUM(DosageQuantity), 2) AS total_dosage
                FROM pesticides
                GROUP BY cvr_number
            """)
        except Exception:
            logger.warning("Could not compute pesticide aggregates")

    def _precompute_worker_safety(self) -> None:
        """Worker safety per company (injury_count can be ranges like '1-5')."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_worker_safety AS
                SELECT
                    cvr_number,
                    year,
                    SUM(TRY_CAST(
                        CASE WHEN injury_count LIKE '%-%'
                            THEN SPLIT_PART(injury_count, '-', 1)
                            ELSE injury_count
                        END AS INTEGER
                    )) AS total_injuries
                FROM worker_safety
                GROUP BY cvr_number, year
                ORDER BY cvr_number, year
            """)
        except Exception:
            logger.warning("Could not compute worker safety aggregates")

    def _precompute_work_permits(self) -> None:
        """Work permits per company."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_work_permits AS
                SELECT
                    company_id AS cvr_number,
                    year,
                    SUM(first_permits_count) AS total_permits
                FROM work_permits
                GROUP BY company_id, year
                ORDER BY company_id, year
            """)
        except Exception:
            logger.warning("Could not compute work permit aggregates")

    def _precompute_env(self) -> None:
        """Environmental analysis: BNBO status counts per company.
        Note: coverage_pct and area_m2 are all NaN. Only bnbo_status_count has real data."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_environment AS
                SELECT
                    cvr_number,
                    COUNT(*) AS total_fields,
                    SUM(bnbo_status_count) AS total_bnbo_statuses,
                    COUNT(CASE WHEN bnbo_status_count > 0 THEN 1 END) AS bnbo_affected_fields
                FROM env_fields
                WHERE cvr_number IS NOT NULL
                GROUP BY cvr_number
                HAVING SUM(bnbo_status_count) > 0
            """)
            cnt = self.conn.execute("SELECT COUNT(*) FROM company_environment").fetchone()[0]
            logger.info(f"Environmental aggregates: {cnt:,} companies with BNBO data")
        except Exception:
            logger.warning("Could not compute environmental aggregates")

    def _precompute_nitrogen(self) -> None:
        """Nitrogen leaching per company per year."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_nitrogen AS
                SELECT
                    cvr_number,
                    year,
                    ROUND(SUM(nitrogen_washout_kg_ha * area_ha::DOUBLE), 1) AS total_n_leached_kg,
                    ROUND(SUM(nitrogen_washout_kg_ha * area_ha::DOUBLE)
                        / NULLIF(SUM(area_ha::DOUBLE), 0), 2) AS n_leached_kg_per_ha,
                    ROUND(SUM(area_ha::DOUBLE), 1) AS total_area_ha
                FROM nitrogen
                WHERE cvr_number IS NOT NULL
                GROUP BY cvr_number, year
                ORDER BY cvr_number, year
            """)
        except Exception:
            logger.warning("Could not compute nitrogen aggregates")

    def _precompute_inspections(self) -> None:
        """Workplace inspections per company. CVR is stored as DOUBLE (e.g. 12598734.0)."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_inspections AS
                SELECT
                    LPAD(cvr_number::INTEGER::VARCHAR, 8, '0') AS cvr_number,
                    COUNT(*) AS total_inspections,
                    SUM(CASE WHEN decision = 'strakspåbud' THEN 1 ELSE 0 END) AS immediate_orders,
                    SUM(CASE WHEN decision = 'påbud' THEN 1 ELSE 0 END) AS orders,
                    SUM(CASE WHEN decision = 'forbud' THEN 1 ELSE 0 END) AS prohibitions,
                    SUM(case_count) AS total_cases
                FROM inspections
                WHERE cvr_number IS NOT NULL
                GROUP BY LPAD(cvr_number::INTEGER::VARCHAR, 8, '0')
            """)
            cnt = self.conn.execute("SELECT COUNT(*) FROM company_inspections").fetchone()[0]
            logger.info(f"Inspection aggregates: {cnt:,} companies with inspections")
        except Exception:
            logger.warning("Could not compute inspection aggregates")

    def _precompute_subsidies(self) -> None:
        """Subsidies per company from EU subsidy data (stoetteoplysninger).
        CVR column = vat_or_tax_identification_number, amounts in DKK."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_subsidies AS
                SELECT
                    LPAD(vat_or_tax_identification_number, 8, '0') AS cvr_number,
                    ROUND(SUM(TRY_CAST(total_of_the_eu_amount_for_that_beneficiary_dkk AS DOUBLE)), 0)
                        AS total_eu_subsidy_dkk,
                    ROUND(SUM(TRY_CAST(total_of_eagf_amount_for_that_beneficiary_dkk AS DOUBLE)), 0)
                        AS total_eagf_dkk,
                    COUNT(*) AS subsidy_records
                FROM subsidies_eu
                WHERE vat_or_tax_identification_number IS NOT NULL
                GROUP BY vat_or_tax_identification_number
            """)
            cnt = self.conn.execute("SELECT COUNT(*) FROM company_subsidies").fetchone()[0]
            logger.info(f"Subsidy aggregates: {cnt:,} companies with subsidies")
        except Exception:
            logger.warning("Could not compute subsidy aggregates")

    def _precompute_fertiliser(self) -> None:
        """Fertiliser data has NULL n_kvote for all rows — skip for now."""

    # --- File generation ---

    def _generate_basic_files(self) -> int:
        """Generate basic.json for all companies. Returns count."""
        logger.info("Generating basic company files...")

        companies = self.conn.execute("""
            SELECT
                cvr_number::VARCHAR AS cvr,
                company_name,
                current_municipality_name AS municipality,
                current_full_address AS address,
                company_type_description AS company_type,
                latitude,
                longitude
            FROM companies
            WHERE cvr_number IS NOT NULL
            ORDER BY cvr_number
        """).fetchall()

        columns = [
            "cvr",
            "company_name",
            "municipality",
            "address",
            "company_type",
            "latitude",
            "longitude",
        ]
        now = datetime.now(UTC).isoformat()
        count = 0

        for row in companies:
            data = dict(zip(columns, row, strict=False))
            cvr = str(data["cvr"])

            basic = {
                "metadata": {
                    "api_version": "2.0",
                    "generated_at": now,
                    "company_id": cvr,
                    "company_cvr": cvr,
                    "municipality": data["municipality"] or "",
                },
                "company": {
                    "id": cvr,
                    "cvr_number": cvr,
                    "company_name": data["company_name"],
                    "municipality": data["municipality"] or "",
                    "address": data["address"],
                    "company_type": data["company_type"],
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                },
            }

            self.write_json(basic, f"companies/{cvr}/basic.json")
            count += 1

            if count % 10000 == 0:
                logger.info(f"  Basic files: {count}/{len(companies)}")

        logger.info(f"Generated {count} basic company files")
        return count

    def _generate_full_profiles(self) -> int:
        """Generate full company profiles with pageBuilder. Returns count."""
        logger.info("Generating full company profiles...")

        # Companies with any agricultural-related data
        # Build UNION dynamically from tables that were actually loaded
        cvr_sources = {
            "field_production": "SELECT DISTINCT cvr_number FROM field_production",
            "pesticides": "SELECT DISTINCT cvr_number FROM pesticides",
            "worker_safety": "SELECT DISTINCT cvr_number::VARCHAR FROM worker_safety",
            "env_fields": "SELECT DISTINCT cvr_number FROM env_fields WHERE cvr_number IS NOT NULL",
            "nitrogen": "SELECT DISTINCT cvr_number FROM nitrogen WHERE cvr_number IS NOT NULL",
        }
        available_tables = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        existing = {row[0] for row in available_tables}
        union_parts = [sql for table, sql in cvr_sources.items() if table in existing]

        if not union_parts:
            logger.warning("No data tables available for full profiles — skipping")
            return 0

        union_sql = " UNION ".join(union_parts)
        ag_companies = self.conn.execute(f"""
            SELECT
                c.cvr_number::VARCHAR AS cvr,
                c.company_name,
                c.current_municipality_name AS municipality,
                c.current_full_address AS address,
                c.company_type_description AS company_type,
                c.primary_industry_description AS industry
            FROM companies c
            WHERE c.cvr_number::VARCHAR IN ({union_sql})
            ORDER BY c.cvr_number
        """).fetchall()

        columns = ["cvr", "company_name", "municipality", "address", "company_type", "industry"]
        now = datetime.now(UTC).isoformat()
        count = 0

        for row in ag_companies:
            comp = dict(zip(columns, row, strict=False))
            cvr = str(comp["cvr"])

            page_builder = []

            # 1. Company identity
            page_builder.append(
                {
                    "_key": "company-identity",
                    "_type": "infoCard",
                    "title": "Virksomhed identitet",
                    "items": [
                        {"label": "Navn", "value": comp["company_name"]},
                        {"label": "CVR", "value": cvr},
                        {"label": "Adresse", "value": comp["address"]},
                        {"label": "Kommune", "value": comp["municipality"]},
                        {"label": "Type", "value": comp["company_type"]},
                        {"label": "Branche", "value": comp["industry"]},
                    ],
                }
            )

            # 2. Land use KPIs
            section = self._get_field_kpis(cvr)
            if section:
                page_builder.append(section)

            # 3. Crop distribution chart
            section = self._get_crop_chart(cvr)
            if section:
                page_builder.append(section)

            # 4. Field production history chart
            section = self._get_field_history_chart(cvr)
            if section:
                page_builder.append(section)

            # 5. Financial KPIs
            section = self._get_financial_kpis(cvr)
            if section:
                page_builder.append(section)

            # 6. Environmental compliance KPIs (BNBO, wetlands, grukos)
            section = self._get_env_kpis(cvr)
            if section:
                page_builder.append(section)

            # 7. Nitrogen leaching chart
            section = self._get_nitrogen_chart(cvr)
            if section:
                page_builder.append(section)

            # 8. Pesticide summary KPIs
            section = self._get_pesticide_kpis(cvr)
            if section:
                page_builder.append(section)

            # 9. Workplace inspections
            section = self._get_inspection_kpis(cvr)
            if section:
                page_builder.append(section)

            # 10. Worker safety chart
            section = self._get_worker_safety_chart(cvr)
            if section:
                page_builder.append(section)

            # 11. Work permits chart
            section = self._get_work_permits_chart(cvr)
            if section:
                page_builder.append(section)

            # 12. Subsidies KPIs
            section = self._get_subsidy_kpis(cvr)
            if section:
                page_builder.append(section)

            profile = {
                "metadata": {
                    "api_version": "2.0",
                    "generated_at": now,
                    "company_id": cvr,
                    "company_cvr": cvr,
                    "municipality": comp["municipality"] or "",
                    "config_version": "2.0-r2",
                    "data_updated_at": now,
                },
                "pageBuilder": page_builder,
            }

            self.write_json(profile, f"companies/{cvr}.json")
            count += 1

            if count % 5000 == 0:
                logger.info(f"  Full profiles: {count}/{len(ag_companies)}")

        logger.info(f"Generated {count} full company profiles")
        return count

    # --- Component builders ---

    def _get_field_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_field_summary WHERE cvr_number = ?", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))
        return {
            "_key": "land-use-kpis",
            "_type": "kpiGroup",
            "title": "Arealanvendelse nøgletal",
            "kpis": [
                {
                    "key": "total_ha",
                    "label": "Antal Hektar (2024)",
                    "value": data["area_2024_ha"],
                    "format": "number",
                },
                {"key": "fields", "label": "Antal Marker (2024)", "value": data["fields_2024"]},
                {"key": "crops", "label": "Antal Afgrøder (2024)", "value": data["crops_2024"]},
                {
                    "key": "organic_pct",
                    "label": "Økologisk Andel",
                    "value": data["organic_pct_2024"],
                    "format": "percentage",
                },
                {"key": "years_active", "label": "Aktive År", "value": data["years_active"]},
            ],
        }

    def _get_crop_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT crop_type, organic_farming, area_ha FROM company_crops_2024 WHERE cvr_number = ? ORDER BY area_ha DESC LIMIT 15",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        categories = []
        conv_data = []
        org_data = []
        seen = {}
        for crop, organic, area in rows:
            if crop not in seen:
                seen[crop] = len(categories)
                categories.append(crop or "Ukendt")
                conv_data.append(0.0)
                org_data.append(0.0)
            idx = seen[crop]
            if organic:
                org_data[idx] = float(area or 0)
            else:
                conv_data[idx] = float(area or 0)

        return {
            "_key": "land-use-crop-distribution",
            "_type": "horizontalStackedBarChart",
            "title": "Afgrødefordeling (2024)",
            "unit": "ha",
            "data": {
                "xAxis": {"label": "Afgrøde", "values": categories},
                "yAxis": {"label": "Areal (ha)"},
                "series": [
                    {"name": "Konventionel", "data": conv_data},
                    {"name": "Økologisk", "data": org_data},
                ],
            },
        }

    def _get_field_history_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT year, total_area_ha, total_production_hkg FROM company_field_yearly WHERE cvr_number = ? ORDER BY year",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        years = [int(r[0]) for r in rows]
        areas = [float(r[1] or 0) for r in rows]
        production = [float(r[2] or 0) for r in rows]

        series = [{"name": "Areal (ha)", "data": areas, "type": "bar", "yAxis": "left"}]
        if any(p > 0 for p in production):
            series.append(
                {"name": "Produktion (hkg)", "data": production, "type": "line", "yAxis": "right"}
            )

        return {
            "_key": "field-production-history",
            "_type": "comboChart",
            "title": "Markproduktion over tid",
            "data": {
                "xAxis": {"label": "År", "values": years},
                "yAxis": {"label": "Areal (ha)"},
                "series": series,
            },
        }

    def _get_financial_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_financials WHERE cvr_number = ?::INTEGER", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))

        kpis = []
        mapping = [
            ("net_profit", "Nettoresultat (DKK)", "net_profit_loss", "number"),
            ("total_assets", "Aktiver i alt (DKK)", "total_assets", "number"),
            ("total_equity", "Egenkapital (DKK)", "total_equity", "number"),
            ("equity_ratio", "Soliditetsgrad", "equity_ratio", "percentage"),
            ("return_on_assets", "Afkast af aktiver", "return_on_assets", "percentage"),
            ("employees", "Ansatte (gns.)", "average_number_of_employees", "number"),
            ("property", "Materielle anlægsaktiver (DKK)", "property_plant_equipment", "number"),
        ]
        for key, label, col, fmt in mapping:
            val = data.get(col)
            if val is not None:
                kpis.append({"key": key, "label": label, "value": val, "format": fmt})

        if not kpis:
            return None

        return {
            "_key": "financial-kpis",
            "_type": "kpiGroup",
            "title": "Regnskabstal",
            "kpis": kpis,
        }

    def _get_env_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_environment WHERE cvr_number = ?", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))

        kpis = [
            {
                "key": "bnbo_affected_fields",
                "label": "Marker i BNBO-område",
                "value": data["bnbo_affected_fields"],
            },
            {
                "key": "bnbo_statuses",
                "label": "BNBO-statusser",
                "value": data["total_bnbo_statuses"],
            },
            {"key": "total_fields", "label": "Marker i alt (2024)", "value": data["total_fields"]},
        ]

        if not kpis:
            return None

        return {
            "_key": "environmental-compliance-overview",
            "_type": "kpiGroup",
            "title": "Miljøoverholdelse - oversigt",
            "kpis": kpis,
        }

    def _get_nitrogen_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT year, total_n_leached_kg, n_leached_kg_per_ha FROM company_nitrogen WHERE cvr_number = ? ORDER BY year",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        years = [int(r[0]) for r in rows]
        total_n = [float(r[1] or 0) for r in rows]
        n_per_ha = [float(r[2] or 0) for r in rows]

        return {
            "_key": "environment-nitrogen-leaching",
            "_type": "comboChart",
            "title": "Kvælstofudvaskning (total og pr. hektar)",
            "data": {
                "xAxis": {"label": "År", "values": years},
                "yAxis": {"label": "Kg kvælstof"},
                "series": [
                    {
                        "name": "Total N udvasket (kg)",
                        "data": total_n,
                        "type": "bar",
                        "yAxis": "left",
                    },
                    {
                        "name": "N pr. hektar (kg/ha)",
                        "data": n_per_ha,
                        "type": "line",
                        "yAxis": "right",
                    },
                ],
            },
        }

    def _get_pesticide_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_pesticides WHERE cvr_number = ?", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))
        return {
            "_key": "pesticide-summary",
            "_type": "kpiGroup",
            "title": "Pesticidanvendelse (2023-2024)",
            "kpis": [
                {
                    "key": "applications",
                    "label": "Antal Udspredninger",
                    "value": data["total_applications"],
                },
                {
                    "key": "pesticides",
                    "label": "Unikke Pesticider",
                    "value": data["unique_pesticides"],
                },
                {
                    "key": "area",
                    "label": "Behandlet Areal (ha)",
                    "value": data["total_treated_area_ha"],
                    "format": "number",
                },
                {
                    "key": "dosage",
                    "label": "Total Dosering",
                    "value": data["total_dosage"],
                    "format": "number",
                },
            ],
        }

    def _get_inspection_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_inspections WHERE cvr_number = ?", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))

        kpis = [
            {
                "key": "total_inspections",
                "label": "Antal Tilsyn",
                "value": data["total_inspections"],
            },
            {"key": "total_cases", "label": "Antal Sager", "value": data["total_cases"]},
        ]
        if data.get("immediate_orders") and data["immediate_orders"] > 0:
            kpis.append(
                {
                    "key": "immediate_orders",
                    "label": "Strakspåbud",
                    "value": data["immediate_orders"],
                }
            )
        if data.get("orders") and data["orders"] > 0:
            kpis.append({"key": "orders", "label": "Påbud", "value": data["orders"]})
        if data.get("prohibitions") and data["prohibitions"] > 0:
            kpis.append({"key": "prohibitions", "label": "Forbud", "value": data["prohibitions"]})

        return {
            "_key": "workplace-inspections",
            "_type": "kpiGroup",
            "title": "Arbejdstilsynets tilsyn",
            "kpis": kpis,
        }

    def _get_worker_safety_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT year, total_injuries FROM company_worker_safety WHERE cvr_number = ?::BIGINT ORDER BY year",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        return {
            "_key": "worker-safety",
            "_type": "barChart",
            "title": "Arbejdsskader over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(r[0]) for r in rows]},
                "yAxis": {"label": "Antal skader"},
                "series": [{"name": "Skader", "data": [int(r[1]) for r in rows]}],
            },
        }

    def _get_work_permits_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT year, total_permits FROM company_work_permits WHERE cvr_number = ? ORDER BY year",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        return {
            "_key": "work-permits",
            "_type": "barChart",
            "title": "Arbejdstilladelser over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(r[0]) for r in rows]},
                "yAxis": {"label": "Antal tilladelser"},
                "series": [{"name": "Tilladelser", "data": [int(r[1]) for r in rows]}],
            },
        }

    def _get_subsidy_kpis(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_subsidies WHERE cvr_number = ?", [cvr]
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))

        total_eu = data.get("total_eu_subsidy_dkk") or 0
        total_eagf = data.get("total_eagf_dkk") or 0
        if total_eu <= 0 and total_eagf <= 0:
            return None

        kpis = []
        if total_eu > 0:
            kpis.append(
                {
                    "key": "total_eu",
                    "label": "EU-tilskud i alt (DKK)",
                    "value": total_eu,
                    "format": "number",
                }
            )
        if total_eagf > 0:
            kpis.append(
                {
                    "key": "eagf",
                    "label": "EGFL-tilskud (DKK)",
                    "value": total_eagf,
                    "format": "number",
                }
            )
        kpis.append({"key": "records", "label": "Tilskudsposter", "value": data["subsidy_records"]})

        return {
            "_key": "subsidies-kpis",
            "_type": "kpiGroup",
            "title": "EU-tilskud",
            "kpis": kpis,
        }
