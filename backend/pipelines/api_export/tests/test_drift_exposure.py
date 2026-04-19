"""Tests for DriftExposureExporter.

Covers:
- Spatial join attributes each building to the correct kommune polygon.
- Per-kommune JSON contains the expected buildings, rounded.
- index.json reports total_buildings, unmatched count, and
  national_avg_drift_dose_kg.
"""

import contextlib
import json
from pathlib import Path

import duckdb
import pytest

from exporters.drift_exposure import DriftExposureExporter


def _install_spatial(conn: duckdb.DuckDBPyConnection) -> None:
    with contextlib.suppress(duckdb.Error):
        conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")


@pytest.fixture
def conn_with_fixtures() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB seeded with two kommuner and four buildings.

    Kommune 101 covers x in [0,1], y in [0,1]; kommune 102 covers x in [1,2], y in [0,1].
    Four buildings total: 2 inside 101, 1 inside 102, 1 outside both (x=5).
    """
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    conn.execute("""
        CREATE TABLE dagi_kommuner AS
        SELECT * FROM (VALUES
            ('101', 'Kommune A',
             ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
            ('102', 'Kommune B',
             ST_GeomFromText('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'))
        ) AS t(code, name, geometry)
    """)

    conn.execute("""
        CREATE TABLE drift_exposure AS
        SELECT * FROM (VALUES
            ('b1', 'Addr 1', 'residential',    2024, 0.10, 3, 2, 50.0, 1.5, 10.0, FALSE,
             ST_Point(0.25, 0.25)),
            ('b2', 'Addr 2', 'residential',    2024, 0.40, 5, 3, 30.0, 2.5, 75.0, FALSE,
             ST_Point(0.75, 0.75)),
            ('b3', 'Addr 3', 'publicServices', 2024, 0.05, 1, 1, 80.0, 0.5, 25.0, FALSE,
             ST_Point(1.50, 0.50)),
            ('b4', 'Addr 4', 'residential',    2024, 0.20, 2, 2, 60.0, 1.0, 50.0, FALSE,
             ST_Point(5.00, 5.00))
        ) AS t(building_uuid, address, category_group, pesticide_year,
               total_drift_dose_kg, contributing_fields, unique_pesticides,
               nearest_field_distance_m, max_single_drift_pct, exposure_percentile,
               wind_weighted, geometry)
    """)
    return conn


class _StubbedExporter(DriftExposureExporter):
    """Short-circuit R2 discovery so export() operates on pre-seeded tables."""

    def _latest_drift_parquet(self) -> str | None:
        return "inline://drift_exposure"

    def _latest_dagi_kommuner_parquet(self) -> str | None:
        return "inline://dagi_kommuner"

    def load_parquet_table(self, parquet_path: str, table_name: str) -> int:
        # Tables are pre-seeded by the fixture; skip the real CREATE TABLE AS.
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def test_export_writes_per_kommune_and_index(
    conn_with_fixtures: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    exporter = _StubbedExporter(conn=conn_with_fixtures, output_dir=str(tmp_path))

    stats = exporter.export()

    # Two kommuner (101, 102) + one index = 3 files. Building b4 is unmatched.
    assert stats["files_written"] == 3
    assert stats["building_count"] == 4
    assert stats["kommune_count"] == 2

    drift_dir = tmp_path / "pesticides" / "drift-exposure"

    index = json.loads((drift_dir / "index.json").read_text())
    assert index["pesticide_year"] == 2024
    assert index["building_count"] == 4
    assert index["unmatched_count"] == 1
    assert index["kommunekoder"] == ["101", "102"]
    # Avg of 0.10, 0.40, 0.05, 0.20 = 0.1875
    assert index["national_avg_drift_dose_kg"] == pytest.approx(0.1875)

    kommune_101 = json.loads((drift_dir / "101.json").read_text())
    uids = sorted(b["uid"] for b in kommune_101)
    assert uids == ["b1", "b2"]
    b1 = next(b for b in kommune_101 if b["uid"] == "b1")
    assert b1["lat"] == pytest.approx(0.25)
    assert b1["lng"] == pytest.approx(0.25)
    assert b1["pct"] == pytest.approx(10.0)
    assert b1["dose"] == pytest.approx(0.1)

    kommune_102 = json.loads((drift_dir / "102.json").read_text())
    assert [b["uid"] for b in kommune_102] == ["b3"]

    # b4 fell outside any polygon → no file, only counted as unmatched.
    assert not (drift_dir / "999.json").exists()


def test_export_noop_when_drift_parquet_missing(
    tmp_path: Path,
) -> None:
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    class _NoDrift(DriftExposureExporter):
        def _latest_drift_parquet(self) -> str | None:
            return None

    stats = _NoDrift(conn=conn, output_dir=str(tmp_path)).export()
    assert stats == {"files_written": 0}
    assert not (tmp_path / "pesticides").exists()
