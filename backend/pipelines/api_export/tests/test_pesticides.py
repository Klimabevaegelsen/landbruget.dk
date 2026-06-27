"""Tests for PesticidesExporter."""

import json
from pathlib import Path

import duckdb

from exporters.pesticides import PesticidesExporter


def _seed_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE pesticides AS
        SELECT * FROM (VALUES
            ('11111111', 'Alpha', 1.0, 'L', 10.0, 'area_match', 'Aarhus'),
            ('11111111', 'Beta',  2.5, 'kg', 8.0,  'area_match', 'Aarhus'),
            ('22222222', 'Gamma', 3.0, 'L', 12.0, 'area_match', 'Odense')
        ) AS t(cvr_number, PesticideName, DosageQuantity, DosageUnit, AllocatedArea, AllocationMethod, municipality)
    """)
    conn.execute("""
        CREATE TABLE companies AS
        SELECT * FROM (VALUES
            ('11111111', 'Farm One', 'Aarhus'),
            ('22222222', 'Farm Two', 'Odense')
        ) AS t(cvr_number, company_name, current_municipality_name)
    """)


def _legacy_per_company_payloads(conn: duckdb.DuckDBPyConnection, period: str) -> dict[str, dict]:
    """Replicate the pre-batching SQL shape for regression comparison."""
    payloads: dict[str, dict] = {}
    companies = conn.execute("""
        SELECT DISTINCT p.cvr_number, c.company_name, c.current_municipality_name AS municipality
        FROM pesticides p
        LEFT JOIN companies c ON p.cvr_number = c.cvr_number::VARCHAR
        ORDER BY p.cvr_number
    """).fetchall()

    for cvr, company_name, municipality in companies:
        details_result = conn.execute(f"""
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
        detail_columns = [desc[0] for desc in details_result.description]
        details = [
            dict(zip(detail_columns, row, strict=False)) for row in details_result.fetchall()
        ]

        summary_result = conn.execute(f"""
            SELECT
                COUNT(*) AS total_applications,
                COUNT(DISTINCT PesticideName) AS unique_pesticides,
                ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                ROUND(SUM(DosageQuantity), 2) AS total_dosage
            FROM pesticides
            WHERE cvr_number = '{cvr}'
        """)
        summary_columns = [desc[0] for desc in summary_result.description]
        summary_rows = [
            dict(zip(summary_columns, row, strict=False)) for row in summary_result.fetchall()
        ]

        payloads[str(cvr)] = {
            "cvr_number": str(cvr),
            "company_name": company_name,
            "municipality": municipality,
            "summary": summary_rows[0] if summary_rows else {},
            "applications": details,
            "metadata": {
                "generated_at": "__dynamic__",
                "period": period,
            },
        }

    return payloads


def test_per_company_writes_expected_json(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _seed_tables(conn)

    exporter = PesticidesExporter(conn=conn, output_dir=str(tmp_path))

    count = exporter._per_company("2024-2025")

    assert count == 2

    company_one = json.loads((tmp_path / "pesticides" / "companies" / "11111111.json").read_text())
    assert company_one["company_name"] == "Farm One"
    assert company_one["municipality"] == "Aarhus"
    assert company_one["summary"] == {
        "total_applications": 2,
        "total_use_allocations": 2,
        "unique_pesticides": 2,
        "total_treated_area_ha": 18.0,
        "total_dosage": 3.5,
    }
    assert [entry["pesticide_name"] for entry in company_one["applications"]] == ["Alpha", "Beta"]
    assert company_one["use_allocations"] == company_one["applications"]

    company_two = json.loads((tmp_path / "pesticides" / "companies" / "22222222.json").read_text())
    assert company_two["summary"]["total_applications"] == 1
    assert company_two["summary"]["total_use_allocations"] == 1
    assert [entry["pesticide_name"] for entry in company_two["applications"]] == ["Gamma"]


def test_per_company_matches_legacy_query_shape(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _seed_tables(conn)

    exporter = PesticidesExporter(conn=conn, output_dir=str(tmp_path))
    period = "2024-2025"

    expected = _legacy_per_company_payloads(conn, period)

    count = exporter._per_company(period)

    assert count == len(expected)

    for cvr, legacy_payload in expected.items():
        written = json.loads((tmp_path / "pesticides" / "companies" / f"{cvr}.json").read_text())
        assert written["cvr_number"] == legacy_payload["cvr_number"]
        assert written["company_name"] == legacy_payload["company_name"]
        assert written["municipality"] == legacy_payload["municipality"]
        expected_summary = {
            **legacy_payload["summary"],
            "total_use_allocations": legacy_payload["summary"]["total_applications"],
        }
        assert written["summary"] == expected_summary
        assert written["applications"] == legacy_payload["applications"]
        assert written["use_allocations"] == legacy_payload["applications"]
        assert written["metadata"]["period"] == legacy_payload["metadata"]["period"]
        assert isinstance(written["metadata"]["generated_at"], str)
        assert written["metadata"]["generated_at"]
