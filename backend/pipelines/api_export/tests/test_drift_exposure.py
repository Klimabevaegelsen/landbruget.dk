"""Tests for DriftExposureExporter.

Covers:
- Spatial tiling writes buildings into deterministic tile shards.
- index.json reports total building count, tile count, and tile zoom.
- Export tolerates drift geometries in normal WGS84, swapped WGS84, or UTM.
"""

import contextlib
import json
import math
from pathlib import Path

import duckdb
import pytest

from exporters.drift_exposure import DRIFT_TILE_ZOOM, DriftExposureExporter


def _install_spatial(conn: duckdb.DuckDBPyConnection) -> None:
    with contextlib.suppress(duckdb.Error):
        conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")


def _slippy_tile(lat: float, lng: float, zoom: int = DRIFT_TILE_ZOOM) -> tuple[int, int]:
    n = 2**zoom
    x = math.floor(((lng + 180.0) / 360.0) * n)
    lat_rad = math.radians(lat)
    y = math.floor(
        (1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n
    )
    return x, y


@pytest.fixture
def conn_with_fixtures() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB seeded with four drift-exposure buildings."""
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    conn.execute("""
        CREATE TABLE drift_exposure AS
        SELECT * FROM (VALUES
            ('b1', 'Addr 1', 'residential',    2024, 0.10, 3, 2, 50.0, 1.5, 10.0, FALSE,
             ST_Point(8.2500, 55.2500)),
            ('b2', 'Addr 2', 'residential',    2024, 0.40, 5, 3, 30.0, 2.5, 75.0, FALSE,
             ST_Point(8.2510, 55.2510)),
            ('b3', 'Addr 3', 'publicServices', 2024, 0.05, 1, 1, 80.0, 0.5, 25.0, FALSE,
             ST_Point(9.5000, 55.5000)),
            ('b4', 'Addr 4', 'residential',    2024, 0.20, 2, 2, 60.0, 1.0, 50.0, FALSE,
             ST_Point(12.5683, 55.6761))
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

    def load_parquet_table(self, parquet_path: str, table_name: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def test_export_ensures_spatial_is_loaded(
    conn_with_fixtures: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    class _TrackingExporter(_StubbedExporter):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.spatial_loaded = False

        def _ensure_spatial_loaded(self) -> None:
            self.spatial_loaded = True

    exporter = _TrackingExporter(conn=conn_with_fixtures, output_dir=str(tmp_path))

    exporter.export()

    assert exporter.spatial_loaded is True


def test_export_writes_per_tile_and_index(
    conn_with_fixtures: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    exporter = _StubbedExporter(conn=conn_with_fixtures, output_dir=str(tmp_path))

    stats = exporter.export()

    # b1/b2 share a tile; b3 and b4 each land in their own.
    assert stats["files_written"] == 4
    assert stats["building_count"] == 4
    assert stats["tile_count"] == 3

    drift_dir = tmp_path / "pesticides" / "drift-exposure"
    index = json.loads((drift_dir / "index.json").read_text())
    assert index["pesticide_year"] == 2024
    assert index["building_count"] == 4
    assert index["tile_zoom"] == DRIFT_TILE_ZOOM
    assert index["tile_count"] == 3
    assert index["national_avg_drift_dose_kg"] == pytest.approx(0.1875)

    tile_x, tile_y = _slippy_tile(55.25, 8.25)
    tile_payload = json.loads(
        (drift_dir / "tiles" / str(DRIFT_TILE_ZOOM) / str(tile_x) / f"{tile_y}.json").read_text()
    )
    assert sorted(b["uid"] for b in tile_payload) == ["b1", "b2"]
    b1 = next(b for b in tile_payload if b["uid"] == "b1")
    assert b1["lat"] == pytest.approx(55.25)
    assert b1["lng"] == pytest.approx(8.25)
    assert b1["pct"] == pytest.approx(10.0)
    assert b1["dose"] == pytest.approx(0.1)


def test_export_matches_swapped_wgs84_drift_geometry(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    conn.execute("""
        CREATE TABLE drift_exposure AS
        SELECT * FROM (VALUES
            ('b1', 'Addr 1', 'residential', 2024, 0.10, 3, 2, 50.0, 1.5, 10.0, FALSE,
             ST_FlipCoordinates(ST_Point(8.2500, 55.2500)))
        ) AS t(building_uuid, address, category_group, pesticide_year,
               total_drift_dose_kg, contributing_fields, unique_pesticides,
               nearest_field_distance_m, max_single_drift_pct, exposure_percentile,
               wind_weighted, geometry)
    """)

    exporter = _StubbedExporter(conn=conn, output_dir=str(tmp_path))
    stats = exporter.export()

    assert stats["files_written"] == 2
    tile_x, tile_y = _slippy_tile(55.25, 8.25)
    tile_payload = json.loads(
        (
            tmp_path
            / "pesticides"
            / "drift-exposure"
            / "tiles"
            / str(DRIFT_TILE_ZOOM)
            / str(tile_x)
            / f"{tile_y}.json"
        ).read_text()
    )
    assert [b["uid"] for b in tile_payload] == ["b1"]


def test_export_matches_utm_drift_geometry_without_crs_metadata(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    conn.execute("""
        CREATE TABLE drift_exposure AS
        SELECT * FROM (VALUES
            ('b1', 'Addr 1', 'residential', 2024, 0.10, 3, 2, 50.0, 1.5, 10.0, FALSE,
             ST_Transform(ST_Point(8.5000, 55.5000), 'EPSG:4326', 'EPSG:25832', always_xy := true))
        ) AS t(building_uuid, address, category_group, pesticide_year,
               total_drift_dose_kg, contributing_fields, unique_pesticides,
               nearest_field_distance_m, max_single_drift_pct, exposure_percentile,
               wind_weighted, geometry)
    """)

    exporter = _StubbedExporter(conn=conn, output_dir=str(tmp_path))
    stats = exporter.export()

    assert stats["files_written"] == 2
    tile_x, tile_y = _slippy_tile(55.5, 8.5)
    tile_payload = json.loads(
        (
            tmp_path
            / "pesticides"
            / "drift-exposure"
            / "tiles"
            / str(DRIFT_TILE_ZOOM)
            / str(tile_x)
            / f"{tile_y}.json"
        ).read_text()
    )
    assert [b["uid"] for b in tile_payload] == ["b1"]


def test_export_handles_wkb_blob_geometry_from_parquet(tmp_path: Path) -> None:
    """Parquet stores geometry as WKB BLOB; the exporter must cast it to GEOMETRY."""
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    conn.execute("""
        CREATE TABLE drift_exposure AS
        SELECT * FROM (VALUES
            ('b1', 'Addr 1', 'residential', 2024, 0.10, 3, 2, 50.0, 1.5, 10.0, FALSE,
             ST_AsWKB(ST_Point(8.2500, 55.2500)))
        ) AS t(building_uuid, address, category_group, pesticide_year,
               total_drift_dose_kg, contributing_fields, unique_pesticides,
               nearest_field_distance_m, max_single_drift_pct, exposure_percentile,
               wind_weighted, geometry)
    """)

    geom_type = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'drift_exposure' AND column_name = 'geometry'"
    ).fetchone()
    assert geom_type[0] == "BLOB"

    exporter = _StubbedExporter(conn=conn, output_dir=str(tmp_path))
    stats = exporter.export()

    assert stats["files_written"] == 2
    assert stats["building_count"] == 1
    tile_x, tile_y = _slippy_tile(55.25, 8.25)
    tile_payload = json.loads(
        (
            tmp_path
            / "pesticides"
            / "drift-exposure"
            / "tiles"
            / str(DRIFT_TILE_ZOOM)
            / str(tile_x)
            / f"{tile_y}.json"
        ).read_text()
    )
    assert [b["uid"] for b in tile_payload] == ["b1"]


def test_export_noop_when_drift_parquet_missing(tmp_path: Path) -> None:
    conn = duckdb.connect(":memory:")
    _install_spatial(conn)

    class _NoDrift(DriftExposureExporter):
        def _latest_drift_parquet(self) -> str | None:
            return None

    stats = _NoDrift(conn=conn, output_dir=str(tmp_path)).export()
    assert stats == {"files_written": 0}
    assert not (tmp_path / "pesticides").exists()
