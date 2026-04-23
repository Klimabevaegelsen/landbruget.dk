import json
from pathlib import Path

import duckdb

from exporters.municipalities import MunicipalitiesExporter


def _write_parquet(conn: duckdb.DuckDBPyConnection, path: Path, sql: str) -> str:
    conn.execute(f"CREATE OR REPLACE TABLE fixture AS {sql}")
    conn.execute(f"COPY fixture TO '{path}' (FORMAT PARQUET)")
    return str(path)


class _StubMunicipalitiesExporter(MunicipalitiesExporter):
    def __init__(self, *args, path_map: dict[tuple[str, str], str], **kwargs):
        super().__init__(*args, **kwargs)
        self.path_map = path_map

    def latest_r2_parquet(
        self,
        dataset_prefix: str,
        filename: str = "data.parquet",
        *,
        include_unversioned: bool = False,
    ) -> str | None:
        return self.path_map.get((dataset_prefix, filename))

    def latest_r2_match(self, pattern: str) -> str | None:
        return self.path_map.get((pattern, "__match__"))

    def latest_r2_nested_parquet(
        self,
        dataset_prefix_glob: str,
        filename: str = "data.parquet",
    ) -> str | None:
        return self.path_map.get((dataset_prefix_glob, filename))

    def r2_uri(self, path: str) -> str:
        return self.path_map.get((path, "__fixed__"), path)


def test_municipalities_export_restores_frontend_categories(tmp_path: Path) -> None:
    fixture_conn = duckdb.connect(":memory:")
    path_map: dict[tuple[str, str], str] = {}

    companies = _write_parquet(
        fixture_conn,
        tmp_path / "companies.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 'Alpha Farm', 'Aarhus'),
                ('87654321', 'Beta Farm', 'Odense')
        ) AS t(cvr_number, company_name, current_municipality_name)
        """,
    )
    path_map[("gold/cvr_enrichment_companies", "data.parquet")] = companies

    field_production = _write_parquet(
        fixture_conn,
        tmp_path / "field_production.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 100.0, TRUE, 'Wheat', 'Aarhus', 1000.0),
                ('12345678', 2024, 50.0, FALSE, 'Barley', 'Aarhus', 600.0),
                ('87654321', 2024, 80.0, FALSE, 'Corn', 'Odense', 900.0)
        ) AS t(cvr_number, year, area_ha, organic_farming, crop_type, kommune_name, production_estimate_hkg)
        """,
    )
    path_map[("gold/field_production/latest/data.parquet", "__fixed__")] = field_production

    pesticides = _write_parquet(
        fixture_conn,
        tmp_path / "pesticides.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 'Aarhus', 10.0, 40.0, 'Prod A', 'REG001'),
                ('12345678', 'Aarhus', 4.0, 10.0, 'Prod B', 'REG002'),
                ('87654321', 'Odense', 8.0, 30.0, 'Prod C', 'REG003')
        ) AS t(cvr_number, municipality, DosageQuantity, AllocatedArea, PesticideName, PesticideRegistrationNumber)
        """,
    )
    path_map[
        ("gold/pesticide_disaggregation_2023_2024", "pesticide_disaggregation_2023_2024.parquet")
    ] = pesticides

    bmd = _write_parquet(
        fixture_conn,
        tmp_path / "bmd.parquet",
        """
        SELECT * FROM (
            VALUES
                ('REG001', true, false, false, 2.0),
                ('REG002', false, false, true, 1.5),
                ('REG003', false, true, false, 3.0)
        ) AS t(registrerings_nr, contains_pfas, contains_diquat, contains_glyphosate, samlet_belastning)
        """,
    )
    path_map[("silver/bmd/*/pesticide_products.parquet", "__match__")] = bmd

    production_sites = _write_parquet(
        fixture_conn,
        tmp_path / "production_sites.parquet",
        """
        SELECT * FROM (
            VALUES
                ('1001', '12345678', 'Aarhus', 120),
                ('1002', '87654321', 'Odense', 80)
        ) AS t(chr, company_id, municipality, capacity)
        """,
    )
    path_map[("gold/chr", "production_sites.parquet")] = production_sites

    antibiotic_usage = _write_parquet(
        fixture_conn,
        tmp_path / "antibiotic_usage.parquet",
        """
        SELECT * FROM (
            VALUES
                (1001, 2024, '12345678', 365.0, 50.0),
                (1002, 2024, '87654321', 200.0, 20.0)
        ) AS t(chr, year, cvr_number, animal_days, animal_doses)
        """,
    )
    path_map[("silver/chr/*/antibiotic_usage.parquet", "__match__")] = antibiotic_usage

    worker_safety = _write_parquet(
        fixture_conn,
        tmp_path / "worker_safety.parquet",
        """
        SELECT * FROM (
            VALUES
                (12345678, 2024, '2'),
                (87654321, 2024, '1')
        ) AS t(cvr_number, year, injury_count)
        """,
    )
    path_map[("gold/worker_safety", "worker_safety_clean.parquet")] = worker_safety

    inspections = _write_parquet(
        fixture_conn,
        tmp_path / "inspections.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', DATE '2024-03-15', 'Strakspåbud'),
                ('12345678', DATE '2024-04-15', 'Påbud'),
                ('87654321', DATE '2024-05-20', 'Vejledning')
        ) AS t(cvr_number, date, decision)
        """,
    )
    path_map[("silver/arbejdstilsynet_inspections", "workplace_inspections.parquet")] = inspections

    nitrogen = _write_parquet(
        fixture_conn,
        tmp_path / "nitrogen.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 40.0, 10.0),
                ('87654321', 2024, 20.0, 8.0)
        ) AS t(cvr_number, year, area_ha, nitrogen_washout_kg_ha)
        """,
    )
    path_map[("gold/nles5_nitrogen_estimation_nitrogen_estimates", "data.parquet")] = nitrogen

    exporter = _StubMunicipalitiesExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        path_map=path_map,
    )

    stats = exporter.export()

    assert stats["files_written"] >= 11

    all_payload = json.loads((tmp_path / "municipalities" / "rankings" / "all.json").read_text())
    assert set(all_payload["rankings"]) == {
        "land_use",
        "organic_farming",
        "production",
        "pesticide_burden",
        "pesticide_pfas",
        "pesticide_glyphosate",
        "antibiotic_usage",
        "environmental",
        "worker_safety",
        "incidents",
    }

    production_payload = json.loads(
        (tmp_path / "municipalities" / "rankings" / "production.json").read_text()
    )
    assert production_payload["rankings"]["production"][0]["metric"] == "total_animal_capacity"

    details_payload = json.loads(
        (tmp_path / "municipalities" / "details" / "Aarhus_production.json").read_text()
    )
    assert details_payload["metric"] == "total_animal_capacity"
    assert details_payload["companies"][0]["company_name"] == "Alpha Farm"

    organic_details = json.loads(
        (tmp_path / "municipalities" / "details" / "Aarhus_organic_farming.json").read_text()
    )
    assert organic_details["metric"] == "organic_farming_percentage"
