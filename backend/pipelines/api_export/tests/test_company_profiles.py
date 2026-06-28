import json
from pathlib import Path

import duckdb

from exporters.company_profiles import CompanyProfilesExporter


class _StubCompanyProfilesExporter(CompanyProfilesExporter):
    def _load_tables(self) -> None:
        return


def _block_map(profile: dict) -> dict:
    return {block["_key"]: block for block in profile["pageBuilder"]}


def _load_fixture_data(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE companies AS
        SELECT * FROM (
            VALUES
                (
                    '12345678',
                    'Alpha Farm',
                    'Aarhus',
                    'Farm Road 1',
                    'ApS',
                    'Landbrug',
                    56.15,
                    10.20
                ),
                (
                    '87654321',
                    'Beta Agro',
                    'Odense',
                    'Field Lane 2',
                    'A/S',
                    'Landbrug',
                    55.40,
                    10.38
                )
        ) AS t(
            cvr_number,
            company_name,
            current_municipality_name,
            current_full_address,
            company_type_description,
            primary_industry_description,
            latitude,
            longitude
        )
    """)

    conn.execute("""
        CREATE TABLE field_production AS
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 120.0, TRUE, 'Wheat', 'Aarhus', 1400.0)
        ) AS t(
            cvr_number,
            year,
            area_ha,
            organic_farming,
            crop_type,
            kommune_name,
            production_estimate_hkg
        )
    """)

    conn.execute("""
        CREATE TABLE agricultural_fields AS
        SELECT * FROM (
            VALUES
                ('f1', '12345678', 'Wheat', 40.0, 2024, 56.16, 10.21),
                ('f2', '12345678', 'Barley', 35.0, 2024, 56.14, 10.19),
                ('f3', '87654321', 'Rye', 20.0, 2024, 55.41, 10.39)
        ) AS t(
            field_id,
            cvr_number,
            crop_name,
            area_ha,
            year,
            latitude,
            longitude
        )
    """)

    conn.execute("""
        CREATE TABLE financials AS
        SELECT * FROM (
            VALUES
                (
                    12345678,
                    150000.0,
                    300000.0,
                    900000.0,
                    450000.0,
                    50.0,
                    16.7,
                    8.0,
                    275000.0,
                    '2024-12-31'
                ),
                (
                    12345678,
                    100000.0,
                    240000.0,
                    850000.0,
                    400000.0,
                    47.1,
                    14.5,
                    7.0,
                    250000.0,
                    '2023-12-31'
                )
        ) AS t(
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
        )
    """)

    conn.execute("""
        CREATE TABLE subsidies_eu AS
        SELECT * FROM (
            VALUES
                ('12345678', 2024, 50000.0, 10000.0, 5000.0, FALSE),
                ('12345678', 2023, 45000.0, 9000.0, 4000.0, FALSE)
        ) AS t(
            cvr,
            regnskabsaar,
            eagf_dkk,
            eafrd_dkk,
            medfinansiering_dkk,
            is_summary_row
        )
    """)

    conn.execute("""
        CREATE TABLE production_sites AS
        SELECT * FROM (
            VALUES
                ('1001', '12345678', 120.0, '15', 'Aarhus', 56.15, 10.20, 'Svinebrug Nord')
        ) AS t(
            chr,
            company_id,
            capacity,
            main_species_code,
            municipality,
            latitude,
            longitude,
            production_site_name
        )
    """)

    conn.execute("""
        CREATE TABLE herd_sizes AS
        SELECT * FROM (
            VALUES
                (1, 1001, 110.0, '15')
        ) AS t(
            herd_number,
            chr,
            count,
            species_code
        )
    """)

    conn.execute("""
        CREATE TABLE antibiotic_usage AS
        SELECT * FROM (
            VALUES
                (1001, 2024, '12345678', 365.0, 50.0),
                (1001, 2023, '12345678', 300.0, 40.0)
        ) AS t(
            chr,
            year,
            cvr_number,
            animal_days,
            animal_doses
        )
    """)

    conn.execute("""
        CREATE TABLE pig_movements AS
        SELECT * FROM (
            VALUES
                (1001, DATE '2024-03-15', 250.0, FALSE),
                (1001, DATE '2023-06-10', 200.0, FALSE)
        ) AS t(
            sender_chr_number,
            movement_date,
            total_animals,
            is_deleted
        )
    """)

    conn.execute("""
        CREATE TABLE persons AS
        SELECT * FROM (
            VALUES
                (
                    'p1',
                    1,
                    'PERSON',
                    'Jane Director',
                    'Aarhus',
                    8000,
                    'Aarhus',
                    'company-1',
                    12345678,
                    'DIREKTØR',
                    'Direktør',
                    '2020-01-01',
                    NULL,
                    TRUE,
                    TRUE,
                    FALSE,
                    '2026-04-22T00:00:00Z'
                ),
                (
                    'p2',
                    2,
                    'PERSON',
                    'John Owner',
                    'Odense',
                    5000,
                    'Odense',
                    'company-1',
                    12345678,
                    'REEL EJER',
                    'Reel Ejer',
                    '2021-01-01',
                    NULL,
                    TRUE,
                    FALSE,
                    TRUE,
                    '2026-04-22T00:00:00Z'
                )
        ) AS t(
            person_uuid,
            unit_number,
            person_type,
            current_name,
            current_city,
            current_postal_code,
            current_municipality,
            company_uuid,
            cvr_number,
            role,
            role_formatted,
            role_start_date,
            role_end_date,
            is_current_role,
            is_leadership,
            is_owner,
            processing_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE employment AS
        SELECT * FROM (
            VALUES
                (12345678, 2024, 1, 10, 8.5, 12, 'monthly', TIMESTAMP '2024-02-01 00:00:00'),
                (12345678, 2024, 2, 12, 10.0, 14, 'monthly', TIMESTAMP '2024-03-01 00:00:00'),
                (12345678, 2024, 2, 13, 10.5, 15, 'replacement_monthly', TIMESTAMP '2024-03-02 00:00:00'),
                (87654321, 2024, 3, 6, 5.0, 7, 'monthly', TIMESTAMP '2024-04-01 00:00:00')
        ) AS t(
            cvr_number,
            year,
            month,
            total_employees,
            full_time_equivalent,
            employees_including_owners,
            employment_type,
            processing_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE worker_safety AS
        SELECT * FROM (
            VALUES
                (12345678, 2023, '1-5'),
                (12345678, 2024, '2'),
                (87654321, 2024, '1')
        ) AS t(
            cvr_number,
            year,
            injury_count
        )
    """)

    conn.execute("""
        CREATE TABLE work_permits AS
        SELECT * FROM (
            VALUES
                ('12345678', 2023, 1),
                ('12345678', 2024, 3),
                ('87654321', 2024, 2)
        ) AS t(
            company_id,
            year,
            first_permits_count
        )
    """)

    conn.execute("""
        CREATE TABLE inspections AS
        SELECT * FROM (
            VALUES
                (12345678.0, 'strakspåbud', 2),
                (12345678.0, 'påbud', 1),
                (87654321.0, 'påbud', 1)
        ) AS t(
            cvr_number,
            decision,
            case_count
        )
    """)

    conn.execute("""
        CREATE TABLE env_fields AS
        SELECT * FROM (
            VALUES
                ('12345678', 2023, 2, 4.0, 1.5, 5000.0, 80000.0, 30000.0),
                ('12345678', 2024, 1, 6.0, 2.0, 10000.0, 120000.0, 40000.0),
                ('87654321', 2024, 1, 2.0, 1.0, 2000.0, 50000.0, 15000.0)
        ) AS t(
            cvr_number,
            year,
            bnbo_status_count,
            bnbo_action_required_hectares,
            bnbo_completed_hectares,
            field_bnbo_water_covered_m2,
            field_wetland_total_m2,
            field_wetland_water_covered_m2
        )
    """)

    conn.execute("""
        CREATE TABLE nitrogen AS
        SELECT * FROM (
            VALUES
                ('12345678', 2023, 100.0, 30.0),
                ('12345678', 2024, 110.0, 35.0),
                ('87654321', 2024, 50.0, 20.0)
        ) AS t(
            cvr_number,
            year,
            area_ha,
            nitrogen_washout_kg_ha
        )
    """)

    conn.execute("""
        CREATE TABLE pesticides AS
        SELECT * FROM (
            VALUES
                ('12345678', 'Herbicide A', 50.0, 5.0, '111'),
                ('12345678', 'Herbicide B', 30.0, 3.0, '222')
        ) AS t(
            cvr_number,
            PesticideName,
            AllocatedArea,
            DosageQuantity,
            PesticideRegistrationNumber
        )
    """)

    conn.execute("""
        CREATE TABLE bmd_products AS
        SELECT * FROM (
            VALUES
                ('111', 2.5, TRUE, FALSE, FALSE),
                ('222', 1.0, FALSE, TRUE, FALSE)
        ) AS t(
            registrerings_nr,
            samlet_belastning,
            contains_pfas,
            contains_glyphosate,
            contains_diquat
        )
    """)


def test_company_profiles_export_extended_parity_sections(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _load_fixture_data(conn)

    exporter = _StubCompanyProfilesExporter(conn=conn, output_dir=str(tmp_path))

    stats = exporter.export()

    assert stats["basic_files"] == 2
    assert stats["full_files"] == 2

    profile = json.loads((tmp_path / "companies" / "12345678.json").read_text())
    blocks = _block_map(profile)

    assert "company-ownership" in blocks
    assert "company-leadership" in blocks
    assert "company-map-overview" in blocks
    assert "financials-latest-kpis" in blocks
    assert "financials-history" in blocks
    assert "financials-detailed-kpis" in blocks
    assert "subsidies-history-stacked" in blocks
    assert "land-use-field-map" in blocks
    assert "animal-welfare-kpis-overall" in blocks
    assert "animal-welfare-production-species-chart" in blocks
    assert "animal-welfare-antibiotics-usage-chart" in blocks
    assert "animal-welfare-site-map" in blocks
    assert "animal-welfare-sites-iteration" in blocks
    assert "animal-welfare-transport-chart" in blocks
    assert "environmental-compliance-overview" in blocks
    assert "bnbo-environmental-status-chart" in blocks
    assert "wetlands-environmental-status-chart" in blocks
    assert "environmental-action-status-chart" in blocks
    assert "water-coverage-effectiveness-chart" in blocks
    assert "environmental-compliance-kpis" in blocks
    assert "environment-kpis" in blocks
    assert "environment-nitrogen-leaching" in blocks
    assert "environment-nitrogen-per-field" in blocks
    assert "environment-pesticide-load" in blocks
    assert "environment-pesticide-risks" in blocks
    assert "worker-welfare-kpis" in blocks
    assert "worker-welfare-employees-monthly" in blocks
    assert "worker-welfare-injuries" in blocks
    assert "worker-welfare-visas" in blocks

    assert blocks["company-ownership"]["rows"][0]["name"] == "John Owner"
    assert blocks["company-ownership"]["rows"][0]["role"] == "Reel Ejer"
    assert blocks["company-leadership"]["rows"][0]["name"] == "Jane Director"
    assert blocks["company-leadership"]["rows"][0]["role"] == "Direktør"
    assert blocks["company-map-overview"]["data"]["center"] == [10.2, 56.15]
    assert blocks["land-use-field-map"]["data"]["center"] == [10.2, 56.15]
    assert blocks["financials-latest-kpis"]["kpis"][0]["key"] == "net_profit"
    assert blocks["financials-history"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["financials-detailed-kpis"]["rows"][0]["year"] == 2024
    assert blocks["subsidies-history-stacked"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["animal-welfare-kpis-overall"]["kpis"][0]["key"] == "production_site_count"
    assert blocks["animal-welfare-production-species-chart"]["data"]["xAxis"]["values"] == ["Svin"]
    assert blocks["animal-welfare-antibiotics-usage-chart"]["data"]["xAxis"]["values"] == [
        2023,
        2024,
    ]
    assert (
        blocks["animal-welfare-site-map"]["data"]["layers"][0]["data"]["features"][0]["properties"][
            "chr"
        ]
        == "1001"
    )
    assert blocks["animal-welfare-sites-iteration"]["sections"][0]["title"] == "Svinebrug Nord"
    assert blocks["animal-welfare-transport-chart"]["data"]["xAxis"]["values"] == [2023, 2024]

    environmental_kpis = {
        item["key"]: item["value"] for item in blocks["environmental-compliance-kpis"]["kpis"]
    }
    assert environmental_kpis["affected_fields"] == 1
    assert environmental_kpis["bnbo_statuses"] == 1
    assert environmental_kpis["total_fields"] == 1
    assert environmental_kpis["problematic_hectares"] == 14.0
    assert environmental_kpis["dealt_with_hectares"] == 6.0
    assert environmental_kpis["compliance_percentage"] == 30.0
    assert environmental_kpis["water_coverage_percentage"] == 25.0

    overview_kpis = {item["key"]: item["value"] for item in blocks["environment-kpis"]["kpis"]}
    assert overview_kpis["bnbo_affected_fields"] == 1
    assert overview_kpis["total_n_leached_kg"] == 3850.0
    assert overview_kpis["n_leached_kg_per_ha"] == 35.0
    assert overview_kpis["total_burden"] == 3.5
    assert overview_kpis["treated_area_ha"] == 80.0

    assert blocks["environment-nitrogen-per-field"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["environment-nitrogen-per-field"]["data"]["series"][0]["data"] == [30.0, 35.0]
    assert blocks["bnbo-environmental-status-chart"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["bnbo-environmental-status-chart"]["data"]["series"][0]["data"] == [4.0, 6.0]
    assert blocks["bnbo-environmental-status-chart"]["data"]["series"][1]["data"] == [1.5, 2.0]
    assert blocks["bnbo-environmental-status-chart"]["data"]["series"][2]["data"] == [0.5, 1.0]
    assert blocks["wetlands-environmental-status-chart"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["wetlands-environmental-status-chart"]["data"]["series"][0]["data"] == [5.0, 8.0]
    assert blocks["wetlands-environmental-status-chart"]["data"]["series"][1]["data"] == [3.0, 4.0]
    assert blocks["wetlands-environmental-status-chart"]["data"]["series"][2]["data"] == [3.0, 4.0]
    assert blocks["water-coverage-effectiveness-chart"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["water-coverage-effectiveness-chart"]["data"]["series"][0]["data"] == [0.5, 1.0]
    assert blocks["water-coverage-effectiveness-chart"]["data"]["series"][1]["data"] == [3.0, 4.0]

    action_chart = blocks["environmental-action-status-chart"]["data"]
    assert action_chart["yAxis"]["values"] == ["BNBO", "Lavbundsjorde"]
    assert action_chart["series"][0]["data"] == [6.0, 8.0]
    assert action_chart["series"][1]["data"] == [2.0, 4.0]
    assert action_chart["series"][2]["data"] == [1.0, 4.0]

    pesticide_load_kpis = {
        item["key"]: item["value"] for item in blocks["environment-pesticide-load"]["kpis"]
    }
    assert pesticide_load_kpis["use_allocations"] == 2
    assert pesticide_load_kpis["pesticides"] == 2
    assert pesticide_load_kpis["area"] == 80.0
    assert pesticide_load_kpis["dosage"] == 8.0
    assert pesticide_load_kpis["burden"] == 3.5

    pesticide_risk_kpis = {
        item["key"]: item["value"] for item in blocks["environment-pesticide-risks"]["kpis"]
    }
    assert pesticide_risk_kpis["pfas_applications"] == 1
    assert pesticide_risk_kpis["glyphosate_applications"] == 1
    assert pesticide_risk_kpis["diquat_applications"] == 0
    assert pesticide_risk_kpis["unique_pesticides"] == 2

    worker_kpis = {item["key"]: item["value"] for item in blocks["worker-welfare-kpis"]["kpis"]}
    assert worker_kpis["employees"] == 13
    assert worker_kpis["full_time_equivalent"] == 10.5
    assert worker_kpis["employees_including_owners"] == 15
    assert worker_kpis["total_inspections"] == 2
    assert worker_kpis["immediate_orders"] == 1
    assert worker_kpis["orders"] == 1

    employees_chart = blocks["worker-welfare-employees-monthly"]["data"]
    assert employees_chart["xAxis"]["values"] == ["2024-01", "2024-02"]
    assert employees_chart["series"][0]["data"] == [10, 13]
    assert employees_chart["series"][1]["data"] == [8.5, 10.5]

    assert blocks["worker-welfare-injuries"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["worker-welfare-injuries"]["data"]["series"][0]["data"] == [1, 2]
    assert blocks["worker-welfare-visas"]["data"]["xAxis"]["values"] == [2023, 2024]
    assert blocks["worker-welfare-visas"]["data"]["series"][0]["data"] == [1, 3]


def test_company_profiles_include_worker_environment_only_companies(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _load_fixture_data(conn)

    exporter = _StubCompanyProfilesExporter(conn=conn, output_dir=str(tmp_path))
    exporter.export()

    profile = json.loads((tmp_path / "companies" / "87654321.json").read_text())
    blocks = _block_map(profile)

    assert "company-identity" in blocks
    assert "company-map-overview" in blocks
    assert "land-use-field-map" in blocks
    assert "worker-welfare-kpis" in blocks
    assert "worker-welfare-employees-monthly" in blocks
    assert "worker-welfare-injuries" in blocks
    assert "worker-welfare-visas" in blocks
    assert "environmental-compliance-overview" in blocks
    assert "bnbo-environmental-status-chart" in blocks
    assert "wetlands-environmental-status-chart" in blocks
    assert "environmental-action-status-chart" in blocks
    assert "water-coverage-effectiveness-chart" in blocks
    assert "environmental-compliance-kpis" in blocks
    assert "environment-kpis" in blocks
    assert "environment-nitrogen-leaching" in blocks
    assert "environment-nitrogen-per-field" in blocks
    assert "financials-latest-kpis" not in blocks
    assert "animal-welfare-kpis-overall" not in blocks

    worker_kpis = {item["key"]: item["value"] for item in blocks["worker-welfare-kpis"]["kpis"]}
    assert worker_kpis["employees"] == 6
    assert worker_kpis["total_inspections"] == 1
    assert worker_kpis["orders"] == 1

    environmental_kpis = {item["key"]: item["value"] for item in blocks["environment-kpis"]["kpis"]}
    assert environmental_kpis["bnbo_affected_fields"] == 1
    assert environmental_kpis["total_n_leached_kg"] == 1000.0
    assert environmental_kpis["n_leached_kg_per_ha"] == 20.0
    assert blocks["company-map-overview"]["data"]["center"] == [10.38, 55.4]
    assert blocks["land-use-field-map"]["data"]["center"] == [10.39, 55.41]
    action_chart = blocks["environmental-action-status-chart"]["data"]
    assert action_chart["series"][0]["data"] == [2.0, 3.5]
    assert action_chart["series"][1]["data"] == [1.0, 1.5]
    assert action_chart["series"][2]["data"] == [0.2, 1.5]
