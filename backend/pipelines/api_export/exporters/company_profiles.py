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
- gold/field_environmental_analysis_fields_2024/*/data.parquet — BNBO/wetlands/grukos (618k)
- gold/nles5_nitrogen_estimation_nitrogen_estimates/*/data.parquet — nitrogen leaching (when present)
- silver/arbejdstilsynet_inspections/*/workplace_inspections.parquet
- silver/subsidies/20260322_074339/stoetteoplysninger...parquet — EU subsidy payments
- silver/subsidies/20260322_074339/Landbrugsstoette_2023.parquet — 2023 subsidies
"""

import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from common.logging_utils import get_pipeline_logger

from exporters.animal import (
    create_company_animal_summary,
    create_company_transport_summaries,
    create_production_sites,
)
from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.company_profiles")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"
SPECIES_LABELS = {
    "12": "Kvæg",
    "15": "Svin",
}


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


def _species_label(species_code) -> str:
    code = "" if species_code is None else str(species_code)
    return SPECIES_LABELS.get(code, code if code else "Ukendt")


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
            "companies": self.latest_r2_parquet("gold/cvr_enrichment_companies"),
            "financials": self.latest_r2_parquet(
                "gold/cvr_enrichment_financial_statements", "financial_statements.parquet"
            ),
            "field_production": _r2("gold/field_production/latest/data.parquet"),
            "agricultural_fields": self.latest_r2_match(
                "silver/agricultural_fields_*/*/data.parquet"
            ),
            "pesticides": self.latest_r2_parquet(
                "gold/pesticide_disaggregation_2023_2024",
                "pesticide_disaggregation_2023_2024.parquet",
            ),
            "worker_safety": self.latest_r2_parquet(
                "gold/worker_safety", "worker_safety_clean.parquet"
            ),
            "employment": self.latest_r2_parquet("silver/cvr_employment"),
            "work_permits": self.latest_r2_parquet("gold/work_permits", "work_permits.parquet"),
            "production_sites": self.latest_r2_parquet("gold/chr", "production_sites.parquet"),
            "chr_properties": self.latest_r2_match("silver/chr/*/properties.parquet"),
            "chr_property_users": self.latest_r2_match("silver/chr/*/property_users.parquet"),
            "chr_property_owners": self.latest_r2_match("silver/chr/*/property_owners.parquet"),
            "persons": self.latest_r2_parquet("silver/cvr_persons"),
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
            "bmd_products": self.latest_r2_match("silver/bmd/*/pesticide_products.parquet"),
            "env_fields": self.latest_r2_nested_parquet(
                "gold/field_environmental_analysis_fields_*"
            ),
            "nitrogen": self.latest_r2_parquet("gold/nles5_nitrogen_estimation_nitrogen_estimates"),
        }
        for name, path in r2_tables.items():
            if not path:
                logger.warning(f"Could not resolve latest R2 parquet for {name}")
                continue
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(f"Could not load {name} from {path}")

        create_production_sites(self.conn)

        # Files with spaces in name — download via rclone first
        rclone_tables = {
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

        inspections = self.latest_r2_parquet(
            "silver/arbejdstilsynet_inspections", "workplace_inspections.parquet"
        )
        if inspections:
            try:
                self.load_parquet_table(inspections, "inspections")
            except Exception:
                logger.warning(f"Could not load inspections from {inspections}")

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

    def _precompute_aggregates(self) -> None:
        """Pre-compute per-company aggregates to avoid N+1 queries."""
        logger.info("Pre-computing per-company aggregates...")

        self._precompute_field_production()
        self._precompute_field_locations()
        self._precompute_financials()
        self._precompute_pesticides()
        self._precompute_employment()
        self._precompute_worker_safety()
        self._precompute_work_permits()
        self._precompute_governance()
        self._precompute_animal()

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

    def _precompute_field_locations(self) -> None:
        """Field locations for company-level map rendering."""
        if not self._table_exists("agricultural_fields"):
            return

        columns = self._table_columns("agricultural_fields")
        if not {"cvr_number", "field_id"}.issubset(columns):
            return

        latitude_expr = None
        longitude_expr = None
        if {"latitude", "longitude"}.issubset(columns):
            latitude_expr = "TRY_CAST(latitude AS DOUBLE)"
            longitude_expr = "TRY_CAST(longitude AS DOUBLE)"
        elif {"centroid_lat", "centroid_lng"}.issubset(columns):
            latitude_expr = "TRY_CAST(centroid_lat AS DOUBLE)"
            longitude_expr = "TRY_CAST(centroid_lng AS DOUBLE)"
        elif {"centroid_y", "centroid_x"}.issubset(columns):
            latitude_expr = "TRY_CAST(centroid_y AS DOUBLE)"
            longitude_expr = "TRY_CAST(centroid_x AS DOUBLE)"
        elif "geometry" in columns:
            latitude_expr = "TRY_CAST(ST_Y(ST_Centroid(geometry)) AS DOUBLE)"
            longitude_expr = "TRY_CAST(ST_X(ST_Centroid(geometry)) AS DOUBLE)"

        if not latitude_expr or not longitude_expr:
            return

        crop_expr = (
            "CAST(crop_name AS VARCHAR)"
            if "crop_name" in columns
            else "CAST(crop_type AS VARCHAR)"
            if "crop_type" in columns
            else "NULL::VARCHAR"
        )
        year_expr = "TRY_CAST(year AS INTEGER)" if "year" in columns else "NULL::INTEGER"
        area_expr = (
            "TRY_CAST(area_ha AS DOUBLE)"
            if "area_ha" in columns
            else "TRY_CAST(grundbetaling_area_ha AS DOUBLE)"
            if "grundbetaling_area_ha" in columns
            else "NULL::DOUBLE"
        )

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE company_field_locations AS
                SELECT
                    LPAD(TRY_CAST(cvr_number AS BIGINT)::VARCHAR, 8, '0') AS cvr_number,
                    CAST(field_id AS VARCHAR) AS field_id,
                    {crop_expr} AS crop_name,
                    COALESCE({area_expr}, 0) AS area_ha,
                    {year_expr} AS year,
                    {latitude_expr} AS latitude,
                    {longitude_expr} AS longitude
                FROM agricultural_fields
                WHERE cvr_number IS NOT NULL
                  AND {latitude_expr} IS NOT NULL
                  AND {longitude_expr} IS NOT NULL
            """)
        except Exception:
            logger.warning("Could not compute field locations")

    def _precompute_financials(self) -> None:
        """Financial summary — latest report per company plus history."""
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
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_financial_history AS
                WITH ranked AS (
                    SELECT
                        cvr_number::VARCHAR AS cvr_number,
                        EXTRACT(
                            YEAR FROM COALESCE(
                                TRY_CAST(reporting_period_end AS DATE),
                                TRY_CAST(reporting_period_end AS TIMESTAMP)
                            )
                        )::INTEGER AS year,
                        net_profit_loss,
                        gross_profit_loss,
                        total_assets,
                        total_equity,
                        equity_ratio,
                        return_on_assets,
                        average_number_of_employees,
                        property_plant_equipment,
                        reporting_period_end,
                        ROW_NUMBER() OVER (
                            PARTITION BY cvr_number,
                            EXTRACT(
                                YEAR FROM COALESCE(
                                    TRY_CAST(reporting_period_end AS DATE),
                                    TRY_CAST(reporting_period_end AS TIMESTAMP)
                                )
                            )
                            ORDER BY reporting_period_end DESC
                        ) AS row_num
                    FROM financials
                    WHERE reporting_period_end IS NOT NULL
                )
                SELECT
                    cvr_number,
                    year,
                    net_profit_loss,
                    gross_profit_loss,
                    total_assets,
                    total_equity,
                    equity_ratio,
                    return_on_assets,
                    average_number_of_employees,
                    property_plant_equipment
                FROM ranked
                WHERE row_num = 1
                  AND year IS NOT NULL
                ORDER BY cvr_number, year
            """)
        except Exception:
            logger.warning("Could not compute financial aggregates")

    def _precompute_pesticides(self) -> None:
        """Pesticide summary per company."""
        try:
            if self._table_exists("bmd_products"):
                self.conn.execute("""
                    CREATE OR REPLACE TABLE company_pesticides AS
                    SELECT
                        p.cvr_number,
                        COUNT(*) AS total_applications,
                        COUNT(DISTINCT p.PesticideName) AS unique_pesticides,
                        ROUND(SUM(COALESCE(p.AllocatedArea, 0)), 1) AS total_treated_area_ha,
                        ROUND(SUM(COALESCE(p.DosageQuantity, 0)), 2) AS total_dosage,
                        ROUND(SUM(COALESCE(b.samlet_belastning, 0)), 2) AS total_burden,
                        SUM(CASE WHEN COALESCE(b.contains_pfas, FALSE) THEN 1 ELSE 0 END) AS pfas_applications,
                        SUM(CASE WHEN COALESCE(b.contains_glyphosate, FALSE) THEN 1 ELSE 0 END) AS glyphosate_applications,
                        SUM(CASE WHEN COALESCE(b.contains_diquat, FALSE) THEN 1 ELSE 0 END) AS diquat_applications
                    FROM pesticides p
                    LEFT JOIN bmd_products b
                      ON CAST(p.PesticideRegistrationNumber AS VARCHAR) = CAST(b.registrerings_nr AS VARCHAR)
                    GROUP BY p.cvr_number
                """)
            else:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE company_pesticides AS
                    SELECT
                        cvr_number,
                        COUNT(*) AS total_applications,
                        COUNT(DISTINCT PesticideName) AS unique_pesticides,
                        ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                        ROUND(SUM(DosageQuantity), 2) AS total_dosage,
                        0::DOUBLE AS total_burden,
                        0 AS pfas_applications,
                        0 AS glyphosate_applications,
                        0 AS diquat_applications
                    FROM pesticides
                    GROUP BY cvr_number
                """)
        except Exception:
            logger.warning("Could not compute pesticide aggregates")

    def _precompute_employment(self) -> None:
        """Monthly employee history per company."""
        try:
            columns = self._table_columns("employment")
            timestamp_order = (
                "processing_timestamp DESC NULLS LAST"
                if "processing_timestamp" in columns
                else "last_updated DESC NULLS LAST"
                if "last_updated" in columns
                else "0"
            )
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE company_employment_monthly AS
                WITH ranked AS (
                    SELECT
                        LPAD(cvr_number::INTEGER::VARCHAR, 8, '0') AS cvr_number,
                        year,
                        month,
                        total_employees,
                        full_time_equivalent,
                        employees_including_owners,
                        employment_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY cvr_number, year, month
                            ORDER BY
                                CASE
                                    WHEN employment_type = 'replacement_monthly' THEN 0
                                    WHEN employment_type = 'monthly' THEN 1
                                    ELSE 2
                                END,
                                __TIMESTAMP_ORDER__
                        ) AS row_num
                    FROM employment
                    WHERE cvr_number IS NOT NULL
                      AND year IS NOT NULL
                      AND month IS NOT NULL
                )
                SELECT
                    cvr_number,
                    year,
                    month,
                    total_employees,
                    full_time_equivalent,
                    employees_including_owners,
                    employment_type
                FROM ranked
                WHERE row_num = 1
                ORDER BY cvr_number, year, month
            """.replace("__TIMESTAMP_ORDER__", timestamp_order)
            )
        except Exception:
            logger.warning("Could not compute employment aggregates")

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

    def _precompute_governance(self) -> None:
        """Governance roster per company from normalized cvr_persons output."""
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_governance AS
                SELECT DISTINCT
                    LPAD(cvr_number::INTEGER::VARCHAR, 8, '0') AS cvr_number,
                    current_name,
                    current_city,
                    role,
                    role_formatted,
                    COALESCE(is_current_role, true) AS is_current_role,
                    COALESCE(is_leadership, false) AS is_leadership,
                    COALESCE(is_owner, false) AS is_owner
                FROM persons
                WHERE cvr_number IS NOT NULL
                  AND (COALESCE(is_leadership, false) OR COALESCE(is_owner, false))
            """)
            cnt = self.conn.execute("SELECT COUNT(*) FROM company_governance").fetchone()[0]
            logger.info(f"Governance aggregates: {cnt:,} role rows")
        except Exception:
            logger.warning("Could not compute governance aggregates")

    def _precompute_animal(self) -> None:
        """Animal welfare summaries from CHR production, herd, antibiotic, and transport data."""
        try:
            create_company_animal_summary(self.conn)
        except Exception:
            logger.warning("Could not compute company animal summary")

        self._precompute_animal_species()
        self._precompute_animal_sites()
        self._precompute_animal_antibiotics_yearly()
        self._precompute_animal_transport_yearly()

    def _precompute_animal_species(self) -> None:
        if not self._table_exists("production_sites"):
            return

        production_columns = self._table_columns("production_sites")
        if not {"company_id", "chr"}.issubset(production_columns):
            return

        if self._table_exists("herd_sizes") and "main_species_code" in production_columns:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_animal_species AS
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
                )
                SELECT
                    COALESCE(sc.cvr_number, hc.cvr_number) AS cvr_number,
                    COALESCE(sc.species_code, hc.species_code) AS species_code,
                    COALESCE(sc.site_capacity, 0) AS site_capacity,
                    COALESCE(hc.registered_animals, 0) AS registered_animals
                FROM site_capacity sc
                FULL OUTER JOIN herd_capacity hc
                  ON sc.cvr_number = hc.cvr_number
                 AND sc.species_code = hc.species_code
            """)
            return

        if "main_species_code" in production_columns:
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_animal_species AS
                SELECT
                    ps.company_id AS cvr_number,
                    CAST(ps.main_species_code AS VARCHAR) AS species_code,
                    SUM(COALESCE(ps.capacity, 0)) AS site_capacity,
                    0 AS registered_animals
                FROM production_sites ps
                WHERE ps.company_id IS NOT NULL
                  AND ps.main_species_code IS NOT NULL
                GROUP BY 1, 2
            """)

    def _precompute_animal_sites(self) -> None:
        if not self._table_exists("production_sites"):
            return

        production_columns = self._table_columns("production_sites")
        if not {"company_id", "chr"}.issubset(production_columns):
            return

        municipality_column = next(
            (
                column
                for column in ["municipality", "municipality_name"]
                if column in production_columns
            ),
            None,
        )
        latitude_column = "latitude" if "latitude" in production_columns else None
        longitude_column = "longitude" if "longitude" in production_columns else None
        site_name_column = next(
            (
                column
                for column in ["production_site_name", "site_name", "name"]
                if column in production_columns
            ),
            None,
        )
        species_column = (
            "main_species_code"
            if "main_species_code" in production_columns
            else "species_code"
            if "species_code" in production_columns
            else None
        )

        municipality_expr = (
            f"CAST(ps.{municipality_column} AS VARCHAR)" if municipality_column else "NULL::VARCHAR"
        )
        latitude_expr = (
            f"TRY_CAST(ps.{latitude_column} AS DOUBLE)" if latitude_column else "NULL::DOUBLE"
        )
        longitude_expr = (
            f"TRY_CAST(ps.{longitude_column} AS DOUBLE)" if longitude_column else "NULL::DOUBLE"
        )
        site_name_expr = (
            f"CAST(ps.{site_name_column} AS VARCHAR)"
            if site_name_column
            else "('CHR ' || CAST(ps.chr AS VARCHAR))"
        )
        species_expr = (
            f"CAST(ps.{species_column} AS VARCHAR)" if species_column else "NULL::VARCHAR"
        )

        herd_join = ""
        herd_select = "0 AS total_herds, 0 AS total_animals_registered"
        if self._table_exists("herd_sizes"):
            herd_join = """
                LEFT JOIN (
                    SELECT
                        chr,
                        COUNT(DISTINCT herd_number) AS total_herds,
                        SUM(COALESCE(count, 0)) AS total_animals_registered
                    FROM herd_sizes
                    WHERE chr IS NOT NULL
                    GROUP BY 1
                ) herd
                  ON TRY_CAST(ps.chr AS BIGINT) = herd.chr
            """
            herd_select = """
                COALESCE(herd.total_herds, 0) AS total_herds,
                COALESCE(herd.total_animals_registered, 0) AS total_animals_registered
            """

        antibiotic_join = ""
        antibiotic_select = "0 AS total_animal_doses, 0 AS total_animal_days"
        if self._table_exists("antibiotic_usage"):
            antibiotic_join = """
                LEFT JOIN (
                    SELECT
                        chr,
                        SUM(COALESCE(animal_doses, 0)) AS total_animal_doses,
                        SUM(COALESCE(animal_days, 0)) AS total_animal_days
                    FROM antibiotic_usage
                    WHERE chr IS NOT NULL
                    GROUP BY 1
                ) antibiotics
                  ON TRY_CAST(ps.chr AS BIGINT) = antibiotics.chr
            """
            antibiotic_select = """
                COALESCE(antibiotics.total_animal_doses, 0) AS total_animal_doses,
                COALESCE(antibiotics.total_animal_days, 0) AS total_animal_days
            """

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE company_animal_sites AS
            SELECT
                ps.company_id AS cvr_number,
                CAST(ps.chr AS VARCHAR) AS chr_number,
                {site_name_expr} AS site_name,
                {municipality_expr} AS municipality,
                {species_expr} AS species_code,
                COALESCE(ps.capacity, 0) AS capacity,
                {latitude_expr} AS latitude,
                {longitude_expr} AS longitude,
                {herd_select},
                {antibiotic_select}
            FROM production_sites ps
            {herd_join}
            {antibiotic_join}
            WHERE ps.company_id IS NOT NULL
              AND ps.chr IS NOT NULL
        """)

    def _precompute_animal_antibiotics_yearly(self) -> None:
        if not (self._table_exists("production_sites") and self._table_exists("antibiotic_usage")):
            return

        production_columns = self._table_columns("production_sites")
        antibiotic_columns = self._table_columns("antibiotic_usage")
        if not {"company_id", "chr"}.issubset(production_columns):
            return
        if not {"chr", "year", "animal_doses", "animal_days"}.issubset(antibiotic_columns):
            return

        self.conn.execute("""
            CREATE OR REPLACE TABLE company_animal_antibiotics_yearly AS
            SELECT
                ps.company_id AS cvr_number,
                au.year,
                SUM(COALESCE(au.animal_doses, 0)) AS total_animal_doses,
                SUM(COALESCE(au.animal_days, 0)) AS total_animal_days
            FROM production_sites ps
            JOIN antibiotic_usage au
              ON TRY_CAST(ps.chr AS BIGINT) = au.chr
            WHERE ps.company_id IS NOT NULL
              AND au.year IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)

    def _precompute_animal_transport_yearly(self) -> None:
        try:
            create_company_transport_summaries(self.conn)
        except Exception:
            logger.warning("Could not compute animal transport aggregates")

    def _precompute_env(self) -> None:
        """Environmental analysis from field-level BNBO/wetland coverage."""
        if not self._table_exists("env_fields"):
            return

        columns = self._table_columns("env_fields")
        if "cvr_number" not in columns:
            return

        year_expr = "TRY_CAST(year AS INTEGER)" if "year" in columns else "NULL::INTEGER"
        total_fields_expr = "COUNT(*)"
        total_bnbo_statuses_expr = (
            "SUM(COALESCE(bnbo_status_count, 0))" if "bnbo_status_count" in columns else "0"
        )
        bnbo_affected_fields_expr = (
            "COUNT(CASE WHEN COALESCE(bnbo_status_count, 0) > 0 THEN 1 END)"
            if "bnbo_status_count" in columns
            else "0"
        )
        bnbo_action_expr = (
            "ROUND(SUM(COALESCE(bnbo_action_required_hectares, 0)), 2)"
            if "bnbo_action_required_hectares" in columns
            else "ROUND(SUM(COALESCE(field_bnbo_total_m2, 0)) / 10000.0, 2)"
            if "field_bnbo_total_m2" in columns
            else "0.0"
        )
        bnbo_completed_expr = (
            "ROUND(SUM(COALESCE(bnbo_completed_hectares, 0)), 2)"
            if "bnbo_completed_hectares" in columns
            else "0.0"
        )
        bnbo_water_expr = (
            "ROUND(SUM(COALESCE(field_bnbo_water_covered_m2, 0)) / 10000.0, 2)"
            if "field_bnbo_water_covered_m2" in columns
            else "0.0"
        )
        wetlands_total_expr = (
            "ROUND(SUM(COALESCE(field_wetland_total_m2, 0)) / 10000.0, 2)"
            if "field_wetland_total_m2" in columns
            else "0.0"
        )
        wetlands_water_expr = (
            "ROUND(SUM(COALESCE(field_wetland_water_covered_m2, 0)) / 10000.0, 2)"
            if "field_wetland_water_covered_m2" in columns
            else "0.0"
        )
        wetlands_action_expr = (
            "ROUND(SUM(GREATEST(COALESCE(field_wetland_total_m2, 0) - COALESCE(field_wetland_water_covered_m2, 0), 0)) / 10000.0, 2)"
            if {"field_wetland_total_m2", "field_wetland_water_covered_m2"}.issubset(columns)
            else wetlands_total_expr
        )

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE company_environment_yearly AS
                WITH base AS (
                    SELECT
                        LPAD(TRY_CAST(cvr_number AS BIGINT)::VARCHAR, 8, '0') AS cvr_number,
                        {year_expr} AS year,
                        {total_fields_expr} AS total_fields,
                        {total_bnbo_statuses_expr} AS total_bnbo_statuses,
                        {bnbo_affected_fields_expr} AS bnbo_affected_fields,
                        {bnbo_action_expr} AS bnbo_action_required_hectares,
                        {bnbo_completed_expr} AS bnbo_completed_hectares,
                        {bnbo_water_expr} AS bnbo_water_covered_hectares,
                        {wetlands_action_expr} AS wetlands_action_required_hectares,
                        {wetlands_water_expr} AS wetlands_completed_hectares,
                        {wetlands_water_expr} AS wetlands_water_covered_hectares,
                        {wetlands_total_expr} AS wetlands_total_hectares
                    FROM env_fields
                    WHERE cvr_number IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    cvr_number,
                    year,
                    total_fields,
                    total_bnbo_statuses,
                    bnbo_affected_fields,
                    bnbo_action_required_hectares,
                    bnbo_completed_hectares,
                    bnbo_water_covered_hectares,
                    wetlands_action_required_hectares,
                    wetlands_completed_hectares,
                    wetlands_water_covered_hectares,
                    ROUND(
                        bnbo_action_required_hectares + wetlands_action_required_hectares,
                        2
                    ) AS total_problematic_hectares,
                    ROUND(
                        bnbo_completed_hectares + wetlands_completed_hectares,
                        2
                    ) AS total_dealt_with_hectares,
                    ROUND(
                        bnbo_water_covered_hectares + wetlands_water_covered_hectares,
                        2
                    ) AS total_water_covered_hectares,
                    CASE
                        WHEN (
                            bnbo_action_required_hectares + wetlands_action_required_hectares +
                            bnbo_completed_hectares + wetlands_completed_hectares
                        ) > 0
                        THEN ROUND(
                            (bnbo_completed_hectares + wetlands_completed_hectares) * 100.0 /
                            (
                                bnbo_action_required_hectares + wetlands_action_required_hectares +
                                bnbo_completed_hectares + wetlands_completed_hectares
                            ),
                            2
                        )
                        ELSE 100.0
                    END AS compliance_percentage,
                    CASE
                        WHEN (
                            bnbo_action_required_hectares + wetlands_action_required_hectares +
                            bnbo_completed_hectares + wetlands_completed_hectares
                        ) > 0
                        THEN ROUND(
                            (bnbo_water_covered_hectares + wetlands_water_covered_hectares) * 100.0 /
                            (
                                bnbo_action_required_hectares + wetlands_action_required_hectares +
                                bnbo_completed_hectares + wetlands_completed_hectares
                            ),
                            2
                        )
                        ELSE 0.0
                    END AS water_coverage_percentage,
                    wetlands_total_hectares
                FROM base
            """)
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_environment AS
                SELECT *
                FROM company_environment_yearly
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY cvr_number
                    ORDER BY year DESC NULLS LAST
                ) = 1
            """)
            cnt = self.conn.execute("SELECT COUNT(*) FROM company_environment").fetchone()[0]
            logger.info(f"Environmental aggregates: {cnt:,} companies with environmental data")
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

        if not self._table_exists("subsidies_eu"):
            return

        try:
            columns = self._table_columns("subsidies_eu")
            if {"cvr", "regnskabsaar", "eagf_dkk", "eafrd_dkk", "medfinansiering_dkk"}.issubset(
                columns
            ):
                self.conn.execute("""
                    CREATE OR REPLACE TABLE company_subsidies_yearly AS
                    SELECT
                        cvr AS cvr_number,
                        regnskabsaar AS year,
                        SUM(COALESCE(eagf_dkk, 0)) AS eagf_dkk,
                        SUM(COALESCE(eafrd_dkk, 0) + COALESCE(medfinansiering_dkk, 0)) AS other_subsidies_dkk
                    FROM subsidies_eu
                    WHERE cvr IS NOT NULL
                      AND COALESCE(is_summary_row, FALSE) = FALSE
                    GROUP BY cvr, regnskabsaar
                """)
                return

            if {
                "vat_or_tax_identification_number",
                "total_of_eagf_amount_for_that_beneficiary_dkk",
                "total_of_the_eu_amount_for_that_beneficiary_dkk",
            }.issubset(columns):
                self.conn.execute("""
                    CREATE OR REPLACE TABLE company_subsidies_yearly AS
                    SELECT
                        LPAD(vat_or_tax_identification_number, 8, '0') AS cvr_number,
                        2024 AS year,
                        SUM(
                            COALESCE(
                                TRY_CAST(total_of_eagf_amount_for_that_beneficiary_dkk AS DOUBLE),
                                0
                            )
                        ) AS eagf_dkk,
                        SUM(
                            GREATEST(
                                COALESCE(
                                    TRY_CAST(total_of_the_eu_amount_for_that_beneficiary_dkk AS DOUBLE),
                                    0
                                ) - COALESCE(
                                    TRY_CAST(total_of_eagf_amount_for_that_beneficiary_dkk AS DOUBLE),
                                    0
                                ),
                                0
                            )
                        ) AS other_subsidies_dkk
                    FROM subsidies_eu
                    WHERE vat_or_tax_identification_number IS NOT NULL
                    GROUP BY vat_or_tax_identification_number
                """)
                return
        except Exception:
            logger.warning("Could not compute yearly subsidies from subsidies_eu")

        if not self._table_exists("subsidies_2023"):
            return

        try:
            columns = self._table_columns("subsidies_2023")
            if {"cvr", "amount_dkk"}.issubset(columns):
                self.conn.execute("""
                    CREATE OR REPLACE TABLE company_subsidies_yearly AS
                    SELECT
                        cvr AS cvr_number,
                        2023 AS year,
                        SUM(COALESCE(amount_dkk, 0)) AS eagf_dkk,
                        0::DOUBLE AS other_subsidies_dkk
                    FROM subsidies_2023
                    WHERE cvr IS NOT NULL
                    GROUP BY cvr
                """)
        except Exception:
            logger.warning("Could not compute yearly subsidies from subsidies_2023")

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
            "company_employment_monthly": "SELECT DISTINCT cvr_number FROM company_employment_monthly",
            "company_financials": "SELECT DISTINCT cvr_number::VARCHAR FROM company_financials",
            "company_work_permits": "SELECT DISTINCT cvr_number::VARCHAR FROM company_work_permits",
            "company_inspections": "SELECT DISTINCT cvr_number::VARCHAR FROM company_inspections",
            "company_subsidies": "SELECT DISTINCT cvr_number::VARCHAR FROM company_subsidies",
            "company_governance": "SELECT DISTINCT cvr_number FROM company_governance",
            "company_animal_summary": "SELECT DISTINCT cvr_number FROM company_animal_summary",
            "company_animal_sites": "SELECT DISTINCT cvr_number FROM company_animal_sites",
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
                c.primary_industry_description AS industry,
                c.latitude,
                c.longitude
            FROM companies c
            WHERE c.cvr_number::VARCHAR IN ({union_sql})
            ORDER BY c.cvr_number
        """).fetchall()

        columns = [
            "cvr",
            "company_name",
            "municipality",
            "address",
            "company_type",
            "industry",
            "latitude",
            "longitude",
        ]
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

            # 2. Company map
            section = self._get_company_map_overview(comp)
            if section:
                page_builder.append(section)

            # 3. Ownership
            section = self._get_ownership_table(cvr)
            if section:
                page_builder.append(section)

            # 4. Leadership
            section = self._get_leadership_table(cvr)
            if section:
                page_builder.append(section)

            # 5. Animal welfare overview
            section = self._get_animal_overview_kpis(cvr)
            if section:
                page_builder.append(section)

            # 5. Animal production by species
            section = self._get_animal_species_chart(cvr)
            if section:
                page_builder.append(section)

            # 6. Animal antibiotic usage
            section = self._get_animal_antibiotics_chart(cvr)
            if section:
                page_builder.append(section)

            # 7. Animal production site map
            section = self._get_animal_site_map(cvr)
            if section:
                page_builder.append(section)

            # 8. Animal production site details
            section = self._get_animal_sites_iteration(cvr)
            if section:
                page_builder.append(section)

            # 9. Animal transport history
            section = self._get_animal_transport_chart(cvr)
            if section:
                page_builder.append(section)

            # 10. Land use KPIs
            section = self._get_field_kpis(cvr)
            if section:
                page_builder.append(section)

            # 11. Crop distribution chart
            section = self._get_crop_chart(cvr)
            if section:
                page_builder.append(section)

            # 12. Field map
            section = self._get_field_map(cvr)
            if section:
                page_builder.append(section)

            # 13. Field production history chart
            section = self._get_field_history_chart(cvr)
            if section:
                page_builder.append(section)

            # 14. Financial latest KPIs
            section = self._get_financial_latest_kpis(cvr)
            if section:
                page_builder.append(section)

            # 15. Financial history
            section = self._get_financial_history_chart(cvr)
            if section:
                page_builder.append(section)

            # 16. Financial details
            section = self._get_financial_detail_grid(cvr)
            if section:
                page_builder.append(section)

            # 17. Environmental compliance KPIs (BNBO, wetlands, grukos)
            section = self._get_env_kpis(cvr)
            if section:
                page_builder.append(section)

            # 18. BNBO environmental status
            section = self._get_bnbo_environmental_status_chart(cvr)
            if section:
                page_builder.append(section)

            # 19. Wetlands environmental status
            section = self._get_wetlands_environmental_status_chart(cvr)
            if section:
                page_builder.append(section)

            # 20. Environmental action status
            section = self._get_environmental_action_status_chart(cvr)
            if section:
                page_builder.append(section)

            # 21. Water coverage effectiveness
            section = self._get_water_coverage_effectiveness_chart(cvr)
            if section:
                page_builder.append(section)

            # 22. Environmental compliance KPIs
            section = self._get_environmental_compliance_kpis(cvr)
            if section:
                page_builder.append(section)

            # 23. Environmental overview KPIs
            section = self._get_environment_kpis(cvr)
            if section:
                page_builder.append(section)

            # 24. Nitrogen leaching chart
            section = self._get_nitrogen_chart(cvr)
            if section:
                page_builder.append(section)

            # 25. Nitrogen intensity chart
            section = self._get_nitrogen_per_field_chart(cvr)
            if section:
                page_builder.append(section)

            # 26. Pesticide load KPIs
            section = self._get_environment_pesticide_load(cvr)
            if section:
                page_builder.append(section)

            # 27. Pesticide risk KPIs
            section = self._get_environment_pesticide_risks(cvr)
            if section:
                page_builder.append(section)

            # 28. Worker welfare KPIs
            section = self._get_worker_welfare_kpis(cvr)
            if section:
                page_builder.append(section)

            # 29. Employee history
            section = self._get_worker_employees_monthly(cvr)
            if section:
                page_builder.append(section)

            # 30. Worker safety chart
            section = self._get_worker_safety_chart(cvr)
            if section:
                page_builder.append(section)

            # 31. Work permits chart
            section = self._get_work_permits_chart(cvr)
            if section:
                page_builder.append(section)

            # 32. Subsidies history
            section = self._get_subsidy_history_chart(cvr)
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

    def _get_company_map_overview(self, company: dict) -> dict | None:
        latitude = company.get("latitude")
        longitude = company.get("longitude")
        if latitude is None or longitude is None:
            return None

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except (TypeError, ValueError):
            return None

        return {
            "_key": "company-map-overview",
            "_type": "mapChart",
            "title": "Virksomhedens placering",
            "data": {
                "center": [longitude, latitude],
                "zoom": 11,
                "layers": [
                    {
                        "name": "Virksomhed",
                        "type": "circle",
                        "style": "hq_marker",
                        "data": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "crs": {
                                            "type": "name",
                                            "properties": {"name": "EPSG:4326"},
                                        },
                                        "coordinates": [longitude, latitude],
                                    },
                                    "properties": {
                                        "company_name": company.get("company_name") or "",
                                        "cvr": company.get("cvr") or "",
                                        "address": company.get("address") or "",
                                        "municipality": company.get("municipality") or "",
                                    },
                                }
                            ],
                        },
                    }
                ],
            },
        }

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

    def _get_ownership_table(self, cvr: str) -> dict | None:
        return self._get_governance_table(
            cvr,
            key="company-ownership",
            title="Ejerskab",
            where_clause="is_owner",
        )

    def _get_leadership_table(self, cvr: str) -> dict | None:
        return self._get_governance_table(
            cvr,
            key="company-leadership",
            title="Ledelse",
            where_clause="is_leadership",
        )

    def _get_governance_table(
        self,
        cvr: str,
        *,
        key: str,
        title: str,
        where_clause: str,
    ) -> dict | None:
        if not self._table_exists("company_governance"):
            return None

        rows = self.conn.execute(
            f"""
            SELECT
                current_name,
                role_formatted,
                current_city,
                is_current_role
            FROM company_governance
            WHERE cvr_number = ?
              AND {where_clause}
            ORDER BY is_current_role DESC, role_formatted, current_name
            """,
            [cvr],
        ).fetchall()
        if not rows:
            return None

        return {
            "_key": key,
            "_type": "dataGrid",
            "title": title,
            "allowFiltering": False,
            "rows": [
                {
                    "name": row[0] or "Ukendt",
                    "role": row[1] or "Ukendt rolle",
                    "city": row[2] or "",
                    "is_current": "Ja" if row[3] else "Nej",
                }
                for row in rows
            ],
            "columns": [
                {"key": "name", "label": "Navn", "column": "name"},
                {"key": "role", "label": "Rolle", "column": "role"},
                {"key": "city", "label": "By", "column": "city"},
                {"key": "is_current", "label": "Aktuel", "column": "is_current"},
            ],
        }

    def _get_animal_overview_kpis(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_summary"):
            return None
        try:
            rows = self.conn.execute(
                "SELECT * FROM company_animal_summary WHERE cvr_number = ?",
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        desc = [d[0] for d in self.conn.description]
        data = dict(zip(desc, rows[0], strict=False))
        kpis = [
            {
                "key": "production_site_count",
                "label": "Produktionssteder",
                "value": data["production_site_count"],
            },
            {
                "key": "total_capacity",
                "label": "Samlet kapacitet",
                "value": data["total_capacity"],
                "format": "number",
            },
            {"key": "total_herds", "label": "Besætninger", "value": data["total_herds"]},
            {
                "key": "registered_animals",
                "label": "Registrerede dyr",
                "value": data["total_animals_registered"],
                "format": "number",
            },
            {
                "key": "animal_doses",
                "label": "Antibiotikadoser",
                "value": data["total_animal_doses"],
                "format": "number",
            },
            {
                "key": "animal_days",
                "label": "Dyredage",
                "value": data["total_animal_days"],
                "format": "number",
            },
        ]

        if self._table_exists("company_animal_transport_yearly"):
            total_transport = self.conn.execute(
                """
                SELECT SUM(COALESCE(transported_animals, 0))
                FROM company_animal_transport_yearly
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchone()[0]
            if total_transport:
                kpis.append(
                    {
                        "key": "transported_animals",
                        "label": "Transporterede dyr",
                        "value": total_transport,
                        "format": "number",
                    }
                )

        return {
            "_key": "animal-welfare-kpis-overall",
            "_type": "kpiGroup",
            "title": "Dyrevelfærd nøgletal",
            "kpis": kpis,
        }

    def _get_animal_species_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_species"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT species_code, site_capacity, registered_animals
                FROM company_animal_species
                WHERE cvr_number = ?
                ORDER BY site_capacity DESC, registered_animals DESC, species_code
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "animal-welfare-production-species-chart",
            "_type": "barChart",
            "title": "Dyreproduktion fordelt på art",
            "data": {
                "xAxis": {"label": "Dyreart", "values": [_species_label(row[0]) for row in rows]},
                "yAxis": {"label": "Antal dyr"},
                "series": [
                    {"name": "Kapacitet", "data": [float(row[1] or 0) for row in rows]},
                    {
                        "name": "Registrerede dyr",
                        "data": [float(row[2] or 0) for row in rows],
                    },
                ],
            },
        }

    def _get_animal_antibiotics_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_antibiotics_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, total_animal_doses, total_animal_days
                FROM company_animal_antibiotics_yearly
                WHERE cvr_number = ?
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "animal-welfare-antibiotics-usage-chart",
            "_type": "comboChart",
            "title": "Antibiotikaforbrug over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "Forbrug"},
                "series": [
                    {
                        "name": "Antibiotikadoser",
                        "data": [float(row[1] or 0) for row in rows],
                        "type": "bar",
                        "yAxis": "left",
                    },
                    {
                        "name": "Dyredage",
                        "data": [float(row[2] or 0) for row in rows],
                        "type": "line",
                        "yAxis": "right",
                    },
                ],
            },
        }

    def _get_animal_site_map(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_sites"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT chr_number, site_name, municipality, species_code, capacity, latitude, longitude
                FROM company_animal_sites
                WHERE cvr_number = ?
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                ORDER BY chr_number
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        features = []
        latitudes = []
        longitudes = []
        for (
            chr_number,
            site_name,
            municipality,
            species_code,
            capacity,
            latitude,
            longitude,
        ) in rows:
            latitudes.append(float(latitude))
            longitudes.append(float(longitude))
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                        "coordinates": [float(longitude), float(latitude)],
                    },
                    "properties": {
                        "production_site_name": site_name or f"CHR {chr_number}",
                        "chr": chr_number,
                        "municipality": municipality or "",
                        "species": _species_label(species_code),
                        "capacity": float(capacity or 0),
                    },
                }
            )

        return {
            "_key": "animal-welfare-site-map",
            "_type": "mapChart",
            "title": "Kort over produktionssteder",
            "data": {
                "center": [sum(longitudes) / len(longitudes), sum(latitudes) / len(latitudes)],
                "zoom": 8,
                "layers": [
                    {
                        "name": "Produktionssteder",
                        "type": "circle",
                        "style": "production_site_marker",
                        "data": {"type": "FeatureCollection", "features": features},
                    }
                ],
            },
        }

    def _get_animal_sites_iteration(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_sites"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT
                    chr_number,
                    site_name,
                    municipality,
                    species_code,
                    capacity,
                    total_herds,
                    total_animals_registered,
                    total_animal_doses,
                    total_animal_days
                FROM company_animal_sites
                WHERE cvr_number = ?
                ORDER BY chr_number
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        sections = []
        for row in rows:
            chr_number = row[0]
            site_name = row[1] or f"CHR {chr_number}"
            sections.append(
                {
                    "_key": f"animal-site-{chr_number}",
                    "title": site_name,
                    "layout": "stack",
                    "content": [
                        {
                            "_key": f"animal-site-{chr_number}-info",
                            "_type": "infoCard",
                            "title": "Produktionssted",
                            "items": [
                                {"label": "CHR", "value": chr_number},
                                {"label": "Kommune", "value": row[2] or ""},
                                {"label": "Dyreart", "value": _species_label(row[3])},
                            ],
                        },
                        {
                            "_key": f"animal-site-{chr_number}-kpis",
                            "_type": "kpiGroup",
                            "title": "Nøgletal for produktionssted",
                            "kpis": [
                                {
                                    "key": "capacity",
                                    "label": "Kapacitet",
                                    "value": row[4],
                                    "format": "number",
                                },
                                {"key": "herds", "label": "Besætninger", "value": row[5]},
                                {
                                    "key": "animals_registered",
                                    "label": "Registrerede dyr",
                                    "value": row[6],
                                    "format": "number",
                                },
                                {
                                    "key": "animal_doses",
                                    "label": "Antibiotikadoser",
                                    "value": row[7],
                                    "format": "number",
                                },
                                {
                                    "key": "animal_days",
                                    "label": "Dyredage",
                                    "value": row[8],
                                    "format": "number",
                                },
                            ],
                        },
                    ],
                }
            )

        return {
            "_key": "animal-welfare-sites-iteration",
            "_type": "iteratedSection",
            "title": "Produktionssteder i detaljer",
            "iterationConfig": {"layout": "tabs", "titleField": "title"},
            "sections": sections,
        }

    def _get_animal_transport_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_animal_transport_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, transported_animals
                FROM company_animal_transport_yearly
                WHERE cvr_number = ?
                ORDER BY year NULLS LAST
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        x_values = [int(row[0]) if row[0] is not None else "Ukendt periode" for row in rows]
        return {
            "_key": "animal-welfare-transport-chart",
            "_type": "barChart",
            "title": "Dyretransporter over tid",
            "data": {
                "xAxis": {"label": "Periode", "values": x_values},
                "yAxis": {"label": "Antal dyr"},
                "series": [
                    {
                        "name": "Transporterede dyr",
                        "data": [float(row[1] or 0) for row in rows],
                    }
                ],
            },
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

    def _get_field_map(self, cvr: str) -> dict | None:
        if not self._table_exists("company_field_locations"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT field_id, crop_name, area_ha, year, latitude, longitude
                FROM company_field_locations
                WHERE cvr_number = ?
                ORDER BY area_ha DESC, field_id
                LIMIT 200
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        features = []
        latitudes = []
        longitudes = []
        for field_id, crop_name, area_ha, year, latitude, longitude in rows:
            if latitude is None or longitude is None:
                continue
            latitudes.append(float(latitude))
            longitudes.append(float(longitude))
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                        "coordinates": [float(longitude), float(latitude)],
                    },
                    "properties": {
                        "field_id": field_id,
                        "crop_name": crop_name or "Ukendt",
                        "area_ha": float(area_ha or 0),
                        "year": int(year) if year is not None else None,
                    },
                }
            )

        if not features:
            return None

        return {
            "_key": "land-use-field-map",
            "_type": "mapChart",
            "title": "Kort over marker",
            "data": {
                "center": [sum(longitudes) / len(longitudes), sum(latitudes) / len(latitudes)],
                "zoom": 10,
                "layers": [
                    {
                        "name": "Marker",
                        "type": "circle",
                        "style": "field_marker",
                        "data": {"type": "FeatureCollection", "features": features},
                    }
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

    def _get_financial_latest_kpis(self, cvr: str) -> dict | None:
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
            "_key": "financials-latest-kpis",
            "_type": "kpiGroup",
            "title": "Seneste regnskabsnøgletal",
            "kpis": kpis,
        }

    def _get_financial_history_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_financial_history"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, net_profit_loss, total_assets, total_equity
                FROM company_financial_history
                WHERE cvr_number = ?
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "financials-history",
            "_type": "comboChart",
            "title": "Regnskabsudvikling over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "DKK"},
                "series": [
                    {
                        "name": "Nettoresultat",
                        "data": [float(row[1] or 0) for row in rows],
                        "type": "line",
                        "yAxis": "left",
                    },
                    {
                        "name": "Aktiver i alt",
                        "data": [float(row[2] or 0) for row in rows],
                        "type": "bar",
                        "yAxis": "right",
                    },
                    {
                        "name": "Egenkapital",
                        "data": [float(row[3] or 0) for row in rows],
                        "type": "bar",
                        "yAxis": "right",
                    },
                ],
            },
        }

    def _get_financial_detail_grid(self, cvr: str) -> dict | None:
        if not self._table_exists("company_financial_history"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT
                    year,
                    net_profit_loss,
                    gross_profit_loss,
                    total_assets,
                    total_equity,
                    equity_ratio,
                    return_on_assets,
                    average_number_of_employees,
                    property_plant_equipment
                FROM company_financial_history
                WHERE cvr_number = ?
                ORDER BY year DESC
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "financials-detailed-kpis",
            "_type": "dataGrid",
            "title": "Regnskabstal i detaljer",
            "allowFiltering": False,
            "rows": [
                {
                    "year": int(row[0]),
                    "net_profit_loss": float(row[1] or 0),
                    "gross_profit_loss": float(row[2] or 0),
                    "total_assets": float(row[3] or 0),
                    "total_equity": float(row[4] or 0),
                    "equity_ratio": float(row[5] or 0),
                    "return_on_assets": float(row[6] or 0),
                    "average_number_of_employees": float(row[7] or 0),
                    "property_plant_equipment": float(row[8] or 0),
                }
                for row in rows
            ],
            "columns": [
                {"key": "year", "label": "År", "column": "year"},
                {
                    "key": "net_profit_loss",
                    "label": "Nettoresultat",
                    "column": "net_profit_loss",
                    "format": "number",
                },
                {
                    "key": "gross_profit_loss",
                    "label": "Bruttoresultat",
                    "column": "gross_profit_loss",
                    "format": "number",
                },
                {
                    "key": "total_assets",
                    "label": "Aktiver i alt",
                    "column": "total_assets",
                    "format": "number",
                },
                {
                    "key": "total_equity",
                    "label": "Egenkapital",
                    "column": "total_equity",
                    "format": "number",
                },
                {
                    "key": "equity_ratio",
                    "label": "Soliditetsgrad",
                    "column": "equity_ratio",
                    "format": "percentage",
                },
                {
                    "key": "return_on_assets",
                    "label": "Afkast af aktiver",
                    "column": "return_on_assets",
                    "format": "percentage",
                },
                {
                    "key": "average_number_of_employees",
                    "label": "Ansatte (gns.)",
                    "column": "average_number_of_employees",
                    "format": "number",
                },
                {
                    "key": "property_plant_equipment",
                    "label": "Materielle anlægsaktiver",
                    "column": "property_plant_equipment",
                    "format": "number",
                },
            ],
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

    def _get_bnbo_environmental_status_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_environment_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, bnbo_action_required_hectares, bnbo_completed_hectares, bnbo_water_covered_hectares
                FROM company_environment_yearly
                WHERE cvr_number = ?
                  AND (
                    bnbo_action_required_hectares > 0 OR
                    bnbo_completed_hectares > 0 OR
                    bnbo_water_covered_hectares > 0
                  )
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "bnbo-environmental-status-chart",
            "_type": "stackedBarChart",
            "title": "BNBO-status over tid",
            "unit": "ha",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "Hektar"},
                "series": [
                    {"name": "Kræver Handling", "data": [float(row[1] or 0) for row in rows]},
                    {"name": "Gennemført", "data": [float(row[2] or 0) for row in rows]},
                    {
                        "name": "Areal til Klima- eller Miljøprojekter",
                        "data": [float(row[3] or 0) for row in rows],
                    },
                ],
            },
        }

    def _get_wetlands_environmental_status_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_environment_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, wetlands_action_required_hectares, wetlands_completed_hectares, wetlands_water_covered_hectares
                FROM company_environment_yearly
                WHERE cvr_number = ?
                  AND (
                    wetlands_action_required_hectares > 0 OR
                    wetlands_completed_hectares > 0 OR
                    wetlands_water_covered_hectares > 0
                  )
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "wetlands-environmental-status-chart",
            "_type": "stackedBarChart",
            "title": "Lavbundsjorde-status over tid",
            "unit": "ha",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "Hektar"},
                "series": [
                    {"name": "Kræver Handling", "data": [float(row[1] or 0) for row in rows]},
                    {"name": "Gennemført", "data": [float(row[2] or 0) for row in rows]},
                    {
                        "name": "Areal til Klima- eller Miljøprojekter",
                        "data": [float(row[3] or 0) for row in rows],
                    },
                ],
            },
        }

    def _get_environmental_action_status_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_environment"):
            return None
        try:
            row = self.conn.execute(
                """
                SELECT
                    bnbo_action_required_hectares,
                    bnbo_completed_hectares,
                    bnbo_water_covered_hectares,
                    wetlands_action_required_hectares,
                    wetlands_completed_hectares,
                    wetlands_water_covered_hectares
                FROM company_environment
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchone()
        except Exception:
            return None
        if not row:
            return None

        values = [float(value or 0) for value in row]
        if not any(values):
            return None

        return {
            "_key": "environmental-action-status-chart",
            "_type": "horizontalStackedBarChart",
            "title": "Miljøindsats status",
            "unit": "ha",
            "data": {
                "xAxis": {"label": "Hektar", "values": []},
                "yAxis": {"label": "Arealtype", "values": ["BNBO", "Lavbundsjorde"]},
                "series": [
                    {"name": "Kræver Handling", "data": [values[0], values[3]]},
                    {"name": "Gennemført", "data": [values[1], values[4]]},
                    {
                        "name": "Areal til Klima- eller Miljøprojekter",
                        "data": [values[2], values[5]],
                    },
                ],
            },
        }

    def _get_water_coverage_effectiveness_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_environment_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, bnbo_water_covered_hectares, wetlands_water_covered_hectares
                FROM company_environment_yearly
                WHERE cvr_number = ?
                  AND (bnbo_water_covered_hectares > 0 OR wetlands_water_covered_hectares > 0)
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "water-coverage-effectiveness-chart",
            "_type": "stackedBarChart",
            "title": "Klima- og miljøprojekter over tid",
            "unit": "ha",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "Hektar"},
                "series": [
                    {
                        "name": "BNBO Klima- eller Miljøprojekter",
                        "data": [float(row[1] or 0) for row in rows],
                    },
                    {
                        "name": "Lavbundsjorde Klima- eller Miljøprojekter",
                        "data": [float(row[2] or 0) for row in rows],
                    },
                ],
            },
        }

    def _get_environmental_compliance_kpis(self, cvr: str) -> dict | None:
        if not self._table_exists("company_environment"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT
                    total_fields,
                    total_bnbo_statuses,
                    bnbo_affected_fields,
                    total_problematic_hectares,
                    total_dealt_with_hectares,
                    compliance_percentage,
                    water_coverage_percentage
                FROM company_environment
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        (
            total_fields,
            total_bnbo_statuses,
            bnbo_affected_fields,
            total_problematic_hectares,
            total_dealt_with_hectares,
            compliance_percentage,
            water_coverage_percentage,
        ) = rows[0]
        return {
            "_key": "environmental-compliance-kpis",
            "_type": "kpiGroup",
            "title": "Miljøoverholdelse nøgletal",
            "kpis": [
                {
                    "key": "affected_fields",
                    "label": "BNBO-berørte marker",
                    "value": bnbo_affected_fields,
                },
                {
                    "key": "bnbo_statuses",
                    "label": "BNBO-statusposter",
                    "value": total_bnbo_statuses,
                },
                {"key": "total_fields", "label": "Marker i analysen", "value": total_fields},
                {
                    "key": "problematic_hectares",
                    "label": "Areal der kræver handling",
                    "value": float(total_problematic_hectares or 0),
                    "format": "number",
                },
                {
                    "key": "dealt_with_hectares",
                    "label": "Areal håndteret",
                    "value": float(total_dealt_with_hectares or 0),
                    "format": "number",
                },
                {
                    "key": "compliance_percentage",
                    "label": "Overholdelsesgrad",
                    "value": float(compliance_percentage or 0),
                    "format": "percentage",
                },
                {
                    "key": "water_coverage_percentage",
                    "label": "Vanddækning",
                    "value": float(water_coverage_percentage or 0),
                    "format": "percentage",
                },
            ],
        }

    def _get_environment_kpis(self, cvr: str) -> dict | None:
        kpis = []

        if self._table_exists("company_environment"):
            env_row = self.conn.execute(
                """
                SELECT total_fields, bnbo_affected_fields
                FROM company_environment
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchone()
            if env_row:
                kpis.append(
                    {
                        "key": "bnbo_affected_fields",
                        "label": "BNBO-berørte marker",
                        "value": env_row[1],
                    }
                )

        if self._table_exists("company_nitrogen"):
            nitrogen_row = self.conn.execute(
                """
                SELECT total_n_leached_kg, n_leached_kg_per_ha
                FROM company_nitrogen
                WHERE cvr_number = ?
                ORDER BY year DESC
                LIMIT 1
                """,
                [cvr],
            ).fetchone()
            if nitrogen_row:
                kpis.extend(
                    [
                        {
                            "key": "total_n_leached_kg",
                            "label": "Samlet kvælstofudvaskning",
                            "value": float(nitrogen_row[0] or 0),
                            "format": "number",
                        },
                        {
                            "key": "n_leached_kg_per_ha",
                            "label": "Kvælstof pr. hektar",
                            "value": float(nitrogen_row[1] or 0),
                            "format": "number",
                        },
                    ]
                )

        if self._table_exists("company_pesticides"):
            pesticide_row = self.conn.execute(
                """
                SELECT total_burden, total_treated_area_ha
                FROM company_pesticides
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchone()
            if pesticide_row:
                kpis.extend(
                    [
                        {
                            "key": "total_burden",
                            "label": "Pesticidbelastning",
                            "value": float(pesticide_row[0] or 0),
                            "format": "number",
                        },
                        {
                            "key": "treated_area_ha",
                            "label": "Behandlet areal",
                            "value": float(pesticide_row[1] or 0),
                            "format": "number",
                        },
                    ]
                )

        if not kpis:
            return None

        return {
            "_key": "environment-kpis",
            "_type": "kpiGroup",
            "title": "Miljø nøgletal",
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

    def _get_nitrogen_per_field_chart(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                """
                SELECT year, n_leached_kg_per_ha
                FROM company_nitrogen
                WHERE cvr_number = ?
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "environment-nitrogen-per-field",
            "_type": "barChart",
            "title": "Kvælstof pr. hektar over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(r[0]) for r in rows]},
                "yAxis": {"label": "Kg/ha"},
                "series": [
                    {
                        "name": "Kvælstof pr. hektar",
                        "data": [float(r[1] or 0) for r in rows],
                    }
                ],
            },
        }

    def _get_environment_pesticide_load(self, cvr: str) -> dict | None:
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
            "_key": "environment-pesticide-load",
            "_type": "kpiGroup",
            "title": "Pesticidbelastning",
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
                {
                    "key": "burden",
                    "label": "Samlet belastning",
                    "value": data.get("total_burden", 0),
                    "format": "number",
                },
            ],
        }

    def _get_environment_pesticide_risks(self, cvr: str) -> dict | None:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    pfas_applications,
                    glyphosate_applications,
                    diquat_applications,
                    unique_pesticides
                FROM company_pesticides
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        pfas_applications, glyphosate_applications, diquat_applications, unique_pesticides = rows[0]

        return {
            "_key": "environment-pesticide-risks",
            "_type": "kpiGroup",
            "title": "Pesticidrisici",
            "kpis": [
                {
                    "key": "pfas_applications",
                    "label": "PFAS-applikationer",
                    "value": pfas_applications,
                },
                {
                    "key": "glyphosate_applications",
                    "label": "Glyphosat-applikationer",
                    "value": glyphosate_applications,
                },
                {
                    "key": "diquat_applications",
                    "label": "Diquat-applikationer",
                    "value": diquat_applications,
                },
                {
                    "key": "unique_pesticides",
                    "label": "Unikke pesticider",
                    "value": unique_pesticides,
                },
            ],
        }

    def _get_worker_welfare_kpis(self, cvr: str) -> dict | None:
        kpis = []

        if self._table_exists("company_employment_monthly"):
            employment_row = self.conn.execute(
                """
                SELECT total_employees, full_time_equivalent, employees_including_owners
                FROM company_employment_monthly
                WHERE cvr_number = ?
                ORDER BY year DESC, month DESC
                LIMIT 1
                """,
                [cvr],
            ).fetchone()
            if employment_row:
                kpis.extend(
                    [
                        {"key": "employees", "label": "Ansatte", "value": employment_row[0]},
                        {
                            "key": "full_time_equivalent",
                            "label": "Fuldtidsækvivalenter",
                            "value": float(employment_row[1] or 0),
                            "format": "number",
                        },
                        {
                            "key": "employees_including_owners",
                            "label": "Ansatte inkl. ejere",
                            "value": employment_row[2],
                        },
                    ]
                )

        if self._table_exists("company_inspections"):
            inspection_row = self.conn.execute(
                """
                SELECT total_inspections, immediate_orders, orders
                FROM company_inspections
                WHERE cvr_number = ?
                """,
                [cvr],
            ).fetchone()
            if inspection_row:
                kpis.extend(
                    [
                        {
                            "key": "total_inspections",
                            "label": "Arbejdstilsynets tilsyn",
                            "value": inspection_row[0],
                        },
                        {
                            "key": "immediate_orders",
                            "label": "Strakspåbud",
                            "value": inspection_row[1],
                        },
                        {"key": "orders", "label": "Påbud", "value": inspection_row[2]},
                    ]
                )

        if not kpis:
            return None

        return {
            "_key": "worker-welfare-kpis",
            "_type": "kpiGroup",
            "title": "Medarbejderforhold nøgletal",
            "kpis": kpis,
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
            "_key": "worker-welfare-inspections-detail",
            "_type": "kpiGroup",
            "title": "Arbejdstilsynets tilsyn",
            "kpis": kpis,
        }

    def _get_worker_employees_monthly(self, cvr: str) -> dict | None:
        if not self._table_exists("company_employment_monthly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, month, total_employees, full_time_equivalent
                FROM company_employment_monthly
                WHERE cvr_number = ?
                ORDER BY year, month
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        labels = [f"{int(year)}-{int(month):02d}" for year, month, _, _ in rows]
        return {
            "_key": "worker-welfare-employees-monthly",
            "_type": "comboChart",
            "title": "Ansatte måned for måned",
            "data": {
                "xAxis": {"label": "Måned", "values": labels},
                "yAxis": {"label": "Ansatte"},
                "series": [
                    {
                        "name": "Ansatte",
                        "data": [int(r[2] or 0) for r in rows],
                        "type": "bar",
                        "yAxis": "left",
                    },
                    {
                        "name": "Fuldtidsækvivalenter",
                        "data": [float(r[3] or 0) for r in rows],
                        "type": "line",
                        "yAxis": "right",
                    },
                ],
            },
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
            "_key": "worker-welfare-injuries",
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
            "_key": "worker-welfare-visas",
            "_type": "barChart",
            "title": "Arbejdstilladelser over tid",
            "data": {
                "xAxis": {"label": "År", "values": [int(r[0]) for r in rows]},
                "yAxis": {"label": "Antal tilladelser"},
                "series": [{"name": "Tilladelser", "data": [int(r[1]) for r in rows]}],
            },
        }

    def _get_subsidy_history_chart(self, cvr: str) -> dict | None:
        if not self._table_exists("company_subsidies_yearly"):
            return None
        try:
            rows = self.conn.execute(
                """
                SELECT year, eagf_dkk, other_subsidies_dkk
                FROM company_subsidies_yearly
                WHERE cvr_number = ?
                ORDER BY year
                """,
                [cvr],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None

        return {
            "_key": "subsidies-history-stacked",
            "_type": "stackedBarChart",
            "title": "Tilskud over tid",
            "unit": "DKK",
            "data": {
                "xAxis": {"label": "År", "values": [int(row[0]) for row in rows]},
                "yAxis": {"label": "DKK"},
                "series": [
                    {"name": "EAGF", "data": [float(row[1] or 0) for row in rows]},
                    {
                        "name": "Øvrige tilskud",
                        "data": [float(row[2] or 0) for row in rows],
                    },
                ],
            },
        }
