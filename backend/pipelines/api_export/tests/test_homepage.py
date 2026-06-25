import json
from pathlib import Path

import duckdb
import pytest

from exporters import homepage as homepage_module
from exporters.homepage import HomepageExporter


def _write_parquet(conn: duckdb.DuckDBPyConnection, path: Path, sql: str) -> str:
    conn.execute(f"CREATE OR REPLACE TABLE fixture AS {sql}")
    conn.execute(f"COPY fixture TO '{path}' (FORMAT PARQUET)")
    return str(path)


class _StubHomepageExporter(HomepageExporter):
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

    def r2_glob(self, pattern: str) -> list[str]:
        path = self.path_map.get((pattern, "__glob__"))
        return [path] if path else []

    def r2_uri(self, path: str) -> str:
        return self.path_map.get((path, "__fixed__"), path)


def test_homepage_export_writes_audited_statistics_and_all_categories(tmp_path: Path) -> None:
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

    financials = _write_parquet(
        fixture_conn,
        tmp_path / "financials.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 5000000.0, 10000000.0, 12, 4000000.0, 0.4, 0.1),
                ('87654321', 2500000.0, 7000000.0, 7, 2800000.0, 0.4, 0.09)
        ) AS t(cvr_number, net_profit_loss, total_assets, average_number_of_employees, total_equity, equity_ratio, return_on_assets)
        """,
    )
    path_map[("gold/cvr_enrichment_financial_statements", "financial_statements.parquet")] = (
        financials
    )

    field_production = _write_parquet(
        fixture_conn,
        tmp_path / "field_production.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 100.0, TRUE, 'Wheat', 'Aarhus', 1000.0),
                ('12345678', 2024, 60.0, FALSE, 'Barley', 'Aarhus', 650.0),
                ('87654321', 2024, 80.0, FALSE, 'Corn', 'Odense', 700.0)
        ) AS t(cvr_number, year, area_ha, organic_farming, crop_type, kommune_name, production_estimate_hkg)
        """,
    )
    path_map[("gold/field_production/latest/data.parquet", "__fixed__")] = field_production
    path_map[("silver/agricultural_fields_*", "data.parquet")] = field_production

    pesticides = _write_parquet(
        fixture_conn,
        tmp_path / "pesticides.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 'Aarhus', 10.0, 40.0, 'Prod A', 'REG001'),
                ('12345678', 'Aarhus', 8.0, 20.0, 'Prod B', 'REG002'),
                ('87654321', 'Odense', 6.0, 30.0, 'Prod A', 'REG003')
        ) AS t(cvr_number, municipality, DosageQuantity, AllocatedArea, PesticideName, PesticideRegistrationNumber)
        """,
    )
    path_map[
        ("gold/pesticide_disaggregation_2023_2024", "pesticide_disaggregation_2023_2024.parquet")
    ] = pesticides
    path_map[
        ("gold/pesticide_disaggregation_*/*/pesticide_disaggregation_*.parquet", "__glob__")
    ] = pesticides

    company_pesticides = _write_parquet(
        fixture_conn,
        tmp_path / "company_pesticides.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678'),
                ('87654321')
        ) AS t(cvr_number)
        """,
    )
    path_map[("silver/pesticides/*/pesticiddata_*.parquet", "__glob__")] = company_pesticides

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

    work_permits = _write_parquet(
        fixture_conn,
        tmp_path / "work_permits.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 'UA', 5),
                ('87654321', 2024, 'PL', 3)
        ) AS t(company_id, year, nationality, first_permits_count)
        """,
    )
    path_map[("gold/work_permits", "work_permits.parquet")] = work_permits

    production_sites = _write_parquet(
        fixture_conn,
        tmp_path / "production_sites.parquet",
        """
        SELECT * FROM (
            VALUES
                ('1001', '12345678', 120, '15'),
                ('1002', '87654321', 80, '12')
        ) AS t(chr, company_id, capacity, main_species_code)
        """,
    )
    path_map[("gold/chr", "production_sites.parquet")] = production_sites

    herd_sizes = _write_parquet(
        fixture_conn,
        tmp_path / "herd_sizes.parquet",
        """
        SELECT * FROM (
            VALUES
                (1, 1001, 120, '15'),
                (2, 1002, 80, '12')
        ) AS t(herd_number, chr, count, species_code)
        """,
    )
    path_map[("silver/chr/*/herd_sizes*.parquet", "__match__")] = herd_sizes

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

    employment = _write_parquet(
        fixture_conn,
        tmp_path / "employment.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', DATE '2024-01-01', 10),
                ('87654321', DATE '2024-02-01', 5)
        ) AS t(company_id, month_year, employee_count)
        """,
    )
    path_map[("silver/cvr_employment", "data.parquet")] = employment

    incidents = _write_parquet(
        fixture_conn,
        tmp_path / "incidents.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', DATE '2024-03-15', 'Strakspåbud'),
                ('87654321', DATE '2024-05-20', 'Vejledning')
        ) AS t(company_id, date, decision)
        """,
    )
    path_map[("silver/arbejdstilsynet_inspections", "workplace_inspections.parquet")] = incidents

    persons = _write_parquet(
        fixture_conn,
        tmp_path / "persons.parquet",
        """
        SELECT * FROM (
            VALUES
                (
                    12345678,
                    '{"leadership":[{"person":{"unit_number":1},"organization":{"member_data":[{"attributter":[{"vaerdier":[{"vaerdi":"DIREKTØR"}]}]}]}},{"person":{"unit_number":2},"organization":{"member_data":[{"attributter":[{"vaerdier":[{"vaerdi":"REEL EJER"}]}]}]}}]}'
                )
        ) AS t(cvr_number, person_data_json)
        """,
    )
    path_map[("silver/cvr_persons", "data.parquet")] = persons

    transports = _write_parquet(
        fixture_conn,
        tmp_path / "transports.parquet",
        """
        SELECT * FROM (
            VALUES
                (1001, DATE '2024-03-15', 250, 15, false),
                (1002, DATE '2024-04-20', 40, 12, false)
        ) AS t(sender_chr_number, movement_date, total_animals, species_code, is_deleted)
        """,
    )
    path_map[("gold/chr_transportation_analysis", "chr_transportation_analysis.parquet")] = (
        transports
    )
    path_map[("silver/svineflytning/*/movements*.parquet", "__match__")] = transports

    vet_events = _write_parquet(
        fixture_conn,
        tmp_path / "vet_events.parquet",
        "SELECT * FROM (VALUES (1), (2), (3)) AS t(id)",
    )
    path_map[("silver/chr/*/property_vet_events.parquet", "__match__")] = vet_events

    herds = _write_parquet(
        fixture_conn, tmp_path / "herds.parquet", "SELECT * FROM (VALUES (1), (2)) AS t(id)"
    )
    path_map[("silver/chr/*/herds*.parquet", "__match__")] = herds

    pesticide_products = _write_parquet(
        fixture_conn,
        tmp_path / "products.parquet",
        """
        SELECT * FROM (
            VALUES
                ('REG001', true, false, false, 2.0),
                ('REG002', false, false, true, 1.5),
                ('REG003', false, true, false, 3.0)
        ) AS t(registrerings_nr, contains_pfas, contains_diquat, contains_glyphosate, samlet_belastning)
        """,
    )
    path_map[("silver/bmd/*/pesticide_products.parquet", "__match__")] = pesticide_products

    bnbo = _write_parquet(
        fixture_conn,
        tmp_path / "bnbo.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 'Aarhus', 'not_dealt_with', 12.5),
                ('87654321', 'Odense', 'dealt_with', 9.0)
        ) AS t(cvr_number, municipality, bnbo_status, area_ha)
        """,
    )
    path_map[("gold/field_analysis_field_bnbo_intersections_*", "data.parquet")] = bnbo

    wetland_detail = _write_parquet(
        fixture_conn,
        tmp_path / "wetland_detail.parquet",
        """
        SELECT * FROM (
            VALUES
                ('12345678', 'Aarhus', 'not_restored', 14.0, 0.0),
                ('87654321', 'Odense', 'present', 11.0, 6.0)
        ) AS t(cvr_number, municipality, wetlands_status, area_ha, water_covered_hectares)
        """,
    )
    path_map[("gold/field_analysis_field_wetland_intersections_*", "data.parquet")] = wetland_detail

    exporter = _StubHomepageExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        path_map=path_map,
    )

    stats = exporter.export()

    assert stats["files_written"] == 7
    assert stats["animal"] > 0

    statistics_payload = json.loads((tmp_path / "homepage" / "statistics.json").read_text())
    assert statistics_payload["metadata"]["legacy_source_slots"] == 21
    assert statistics_payload["total_data_points"] == sum(
        source["row_count"] for source in statistics_payload["sources"]
    )
    assert statistics_payload["metadata"]["missing_sources"] == 0

    animal_payload = json.loads((tmp_path / "homepage" / "rankings" / "animal.json").read_text())
    assert animal_payload["metadata"]["total_tables"] == 6
    animal_rankings = {ranking["id"]: ranking for ranking in animal_payload["rankings"]}
    assert set(animal_rankings) == {
        "largest_pig_production",
        "largest_cattle_production",
        "highest_antibiotic_usage",
        "most_production_sites",
        "most_transported_pigs",
        "most_transported_cattle",
    }
    assert animal_rankings["largest_pig_production"]["items"]
    assert animal_rankings["most_transported_pigs"]["items"][0]["value"] == 250
    assert animal_rankings["most_transported_cattle"]["items"][0]["value"] == 40

    environment_payload = json.loads(
        (tmp_path / "homepage" / "rankings" / "environment.json").read_text()
    )
    assert environment_payload["metadata"]["total_tables"] == 8
    environment_rankings = {ranking["id"]: ranking for ranking in environment_payload["rankings"]}
    assert set(environment_rankings) == {
        "highest_pesticide_burden",
        "most_pfas_usage",
        "most_glyphosate_usage",
        "most_diquat_usage",
        "most_bnbo_not_dealt_with",
        "most_bnbo_dealt_with",
        "most_wetland_not_restored",
        "most_wetland_restored",
    }
    assert all(ranking.get("status") != "missing" for ranking in environment_payload["rankings"])

    worker_payload = json.loads((tmp_path / "homepage" / "rankings" / "worker.json").read_text())
    assert worker_payload["metadata"]["total_tables"] == 5
    assert all(ranking.get("status") != "missing" for ranking in worker_payload["rankings"])

    all_payload = json.loads((tmp_path / "homepage" / "rankings" / "all.json").read_text())
    assert all_payload["metadata"]["total_tables"] == 26
    assert all(ranking.get("status") != "missing" for ranking in all_payload["rankings"])
    assert environment_rankings["highest_pesticide_burden"]["items"]

    worker_payload = json.loads((tmp_path / "homepage" / "rankings" / "worker.json").read_text())
    assert worker_payload["metadata"]["total_tables"] == 5
    worker_rankings = {ranking["id"]: ranking for ranking in worker_payload["rankings"]}
    assert set(worker_rankings) == {
        "most_employees_worker",
        "most_foreign_workers",
        "most_work_injuries",
        "most_workplace_inspections",
        "most_urgent_violations",
    }
    assert worker_rankings["most_workplace_inspections"]["items"]

    all_payload = json.loads((tmp_path / "homepage" / "rankings" / "all.json").read_text())
    categories = {ranking["category"] for ranking in all_payload["rankings"]}
    assert categories == {"financial", "field", "environment", "worker", "animal"}
    assert len(all_payload["rankings"]) == 26


def test_ranking_from_sql_returns_empty_ranking_for_zero_rows(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE empty_companies (
            cvr_number VARCHAR,
            company_name VARCHAR,
            municipality VARCHAR,
            value DOUBLE
        )
    """)
    exporter = _StubHomepageExporter(conn=conn, output_dir=str(tmp_path), path_map={})

    ranking = exporter._ranking_from_sql(
        sql="""
            SELECT cvr_number, company_name, municipality, value
            FROM empty_companies
            WHERE false
        """,
        count_sql="SELECT count(*) FROM empty_companies",
        id="empty_test_ranking",
        title="Empty test ranking",
        category="test",
        description="A ranking with no rows.",
        unit="companies",
        format_fn=str,
    )

    assert isinstance(ranking, dict)
    assert ranking["items"] == []
    assert ranking["company_count"] == 0
    assert ranking["status"] == "missing"
    assert ranking["note"] == "No rows returned for this ranking."


def test_wrap_rankings_raises_for_none_ranking(tmp_path: Path) -> None:
    exporter = _StubHomepageExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        path_map={},
    )

    with pytest.raises(ValueError, match="index 1"):
        exporter._wrap_rankings(
            [
                {
                    "id": "valid",
                    "items": [],
                },
                None,
            ]
        )


def test_wrap_rankings_accepts_valid_rankings_and_warns_for_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exporter = _StubHomepageExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        path_map={},
    )
    rankings = [
        {
            "id": "populated",
            "items": [{"rank": 1, "value": 10}],
        },
        {
            "id": "missing",
            "items": [],
            "status": "missing",
        },
    ]
    warning_calls = []
    monkeypatch.setattr(homepage_module.logger, "warning", lambda *args: warning_calls.append(args))

    payload = exporter._wrap_rankings(rankings)

    assert payload["rankings"] == rankings
    assert payload["metadata"]["total_tables"] == 2
    assert warning_calls == [
        (
            "Homepage export: %s rankings have no data: %s",
            1,
            ["missing"],
        )
    ]
