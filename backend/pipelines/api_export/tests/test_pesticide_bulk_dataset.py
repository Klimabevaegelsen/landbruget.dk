"""Tests for the pesticide bulk dataset exporter."""

import json
from pathlib import Path

import duckdb

from exporters.pesticide_bulk_dataset import DATASET_PREFIX, PesticideBulkDatasetExporter


class _LocalPesticideBulkExporter(PesticideBulkDatasetExporter):
    def __init__(
        self,
        *,
        conn: duckdb.DuckDBPyConnection,
        output_dir: str,
        year_paths: dict[int, str],
        marker_paths: dict[int, str],
        products_path: str | None,
    ):
        super().__init__(conn=conn, output_dir=output_dir)
        self._year_paths = year_paths
        self._marker_paths = marker_paths
        self._products_path = products_path

    def _discover_disaggregation_years(self) -> dict[int, str]:
        return self._year_paths

    def _latest_marker_path_for_field_year(self, field_year: int) -> str | None:
        return self._marker_paths.get(field_year)

    def _latest_products_path(self) -> str | None:
        return self._products_path


def _write_fixture_parquets(tmp_path: Path) -> tuple[dict[int, str], dict[int, str], str]:
    conn = duckdb.connect(":memory:")

    disaggregation_path = tmp_path / "pesticide_disaggregation_2023_2024.parquet"
    conn.execute("""
        CREATE TABLE disaggregation AS
        SELECT * FROM (VALUES
            ('row-1', '11111111', 'Glyphomax', '18-123', 6.0, 'L', 12.0,
             3.0, 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional', 0.95,
             false, 'field-a', 'Aarhus'),
            ('row-1', '11111111', 'Glyphomax', '18-123', 4.0, 'L', 12.0,
             2.0, 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional', 0.95,
             false, 'field-b', 'Aarhus'),
            ('row-2', '22222222', 'FungiStop', '77-456', 1.5, 'kg', 3.0,
             3.0, 'Partial_Field_Coverage_SingleField', 0.8,
             true, 'field-c', 'Odense')
        ) AS t(
            OriginalPesticideRowID,
            cvr_number,
            PesticideName,
            PesticideRegistrationNumber,
            DosageQuantity,
            DosageUnit,
            AcreageSize,
            AllocatedArea,
            AllocationMethod,
            MatchConfidence,
            IsPartialFieldCoverage,
            field_uuid,
            municipality
        )
    """)
    conn.execute(f"COPY disaggregation TO '{disaggregation_path}' (FORMAT PARQUET)")

    fields_path = tmp_path / "fvm_marker_2024.parquet"
    conn.execute("""
        CREATE TABLE fields AS
        SELECT * FROM (VALUES
            ('field-a', '11111111', '101', 3.0, false, 'Aarhus', NULL),
            ('field-b', '11111111', '101', 2.0, false, 'Aarhus', NULL),
            ('field-c', '22222222', '202', 3.0, true, 'Odense', NULL)
        ) AS t(field_uuid, cvr_number, crop_code, area_ha, organic_farming, municipality, geometry)
    """)
    conn.execute(f"COPY fields TO '{fields_path}' (FORMAT PARQUET)")

    products_path = tmp_path / "pesticide_products.parquet"
    conn.execute("""
        CREATE TABLE products AS
        SELECT * FROM (VALUES
            ('18-123', 'Glyphomax', 'glyphosate', 'approved', false, 2.5),
            ('77-456', 'FungiStop', 'example', 'expired', false, 1.0)
        ) AS t(
            registrerings_nr,
            product_name,
            active_substances,
            approval_status,
            pfas_flag,
            samlet_belastning
        )
    """)
    conn.execute(f"COPY products TO '{products_path}' (FORMAT PARQUET)")

    return {2023: str(disaggregation_path)}, {2024: str(fields_path)}, str(products_path)


def test_export_writes_public_bulk_dataset_without_cvr(tmp_path: Path) -> None:
    year_paths, marker_paths, products_path = _write_fixture_parquets(tmp_path)
    conn = duckdb.connect(":memory:")
    exporter = _LocalPesticideBulkExporter(
        conn=conn,
        output_dir=str(tmp_path / "out"),
        year_paths=year_paths,
        marker_paths=marker_paths,
        products_path=products_path,
    )

    stats = exporter.export()

    assert stats["allocation_years"] == [2023]
    assert stats["field_years"] == [2023]
    assert stats["products_written"] is True

    allocation_path = (
        tmp_path / "out" / DATASET_PREFIX / "use_allocations" / "year=2023" / "part-000.parquet"
    )
    fields_path = tmp_path / "out" / DATASET_PREFIX / "fields" / "year=2023" / "part-000.parquet"
    assert allocation_path.exists()
    assert fields_path.exists()

    allocation_columns = [
        row[0] for row in conn.execute(f"DESCRIBE SELECT * FROM '{allocation_path}'").fetchall()
    ]
    field_columns = [
        row[0] for row in conn.execute(f"DESCRIBE SELECT * FROM '{fields_path}'").fetchall()
    ]
    assert "cvr_number" not in allocation_columns
    assert "cvr_number" not in field_columns
    assert "source_record_hash" in allocation_columns
    assert "holding_crop_period_hash" in allocation_columns

    rows = conn.execute(f"""
        SELECT
            pesticide_name,
            allocated_quantity,
            allocated_cumulative_treated_area_ha,
            average_reported_rate_per_treated_ha,
            matched_field_count,
            is_partial_field_coverage
        FROM '{allocation_path}'
        ORDER BY pesticide_name, allocated_quantity
    """).fetchall()

    assert rows == [
        ("FungiStop", 1.5, 3.0, 0.5, 1, True),
        ("Glyphomax", 4.0, 2.0, 2.0, 2, False),
        ("Glyphomax", 6.0, 3.0, 2.0, 2, False),
    ]


def test_metadata_uses_allocation_language_and_documents_limits(tmp_path: Path) -> None:
    year_paths, marker_paths, products_path = _write_fixture_parquets(tmp_path)
    conn = duckdb.connect(":memory:")
    exporter = _LocalPesticideBulkExporter(
        conn=conn,
        output_dir=str(tmp_path / "out"),
        year_paths=year_paths,
        marker_paths=marker_paths,
        products_path=products_path,
    )

    exporter.export()

    readme = (tmp_path / "out" / DATASET_PREFIX / "README.md").read_text()
    datapackage = json.loads((tmp_path / "out" / DATASET_PREFIX / "datapackage.json").read_text())
    sql = (tmp_path / "out" / DATASET_PREFIX / "examples" / "duckdb.sql").read_text()
    checksums = (tmp_path / "out" / DATASET_PREFIX / "checksums.txt").read_text()

    assert "Rows are not individual spray events" in readme
    assert "1 August YYYY–31 July YYYY+1" in readme
    assert "No row contains the number of times or passes" in readme
    assert "exact normalized CVR and crop-code match" in readme
    assert "do not provide a quantity-weighted allocation-coverage measure" in readme
    assert "CC-BY-4.0" in readme
    assert "Geodatastyrelsen" in readme
    assert "Miljøstyrelsen" in readme
    assert "exclude raw CVR numbers" in datapackage["description"]
    assert datapackage["licenses"][0]["name"] == "CC-BY-4.0"
    assert datapackage["resources"][0]["name"] == "use_allocations"
    assert "data.landbruget.dk" not in sql
    assert "use_allocations/year=*/part-000.parquet" in sql
    assert "use_allocations/year=2023/part-000.parquet" in checksums
    assert "applications/" not in readme
    assert "application_date" not in readme
    assert "application date" not in readme
