"""Silver merge, geometry, and GeoParquet contract tests."""

from __future__ import annotations

import json
import struct
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

import child_receptors.silver.build as silver_build
from child_receptors.config import OSM_REGIONS, SOURCE_FILENAMES
from child_receptors.silver.build import build_silver, dedupe_playgrounds, write_parquet


def _row(receptor_id: str, source: str, east: float, *, name: str | None = None) -> dict:
    return {
        "receptor_id": receptor_id,
        "receptor_type": "playground",
        "subtype": source,
        "name": name,
        "ownership": None,
        "kommune_kode": None,
        "address": None,
        "dawa_id": None,
        "cvr": None,
        "p_number": None,
        "is_active": True,
        "source": source,
        "matched_sources": [source],
        "source_updated_at": None,
        "utm_e": east,
        "utm_n": 6170000.0,
        "lon": 12.0,
        "lat": 55.0,
        "_fetch_timestamp": "2026-10-10T00:00:00Z",
        "_source_crs": "EPSG:25832",
        "geometry": struct.pack("<BIdd", 1, 1, east, 6170000.0),
    }


def test_playground_dedupe_priority_greedy_and_25_meter_threshold() -> None:
    rows = [
        _row("geofa:1", "geofa", 500000, name="GeoFA name"),
        _row("bbr:2", "bbr", 500010),
        _row("osm:node/3", "osm", 500020, name="OSM name"),
        _row("osm:node/4", "osm", 500030),
    ]
    output, stats = dedupe_playgrounds(rows)
    assert len(output) == 2
    kept = next(row for row in output if row["receptor_id"] == "geofa:1")
    assert kept["matched_sources"] == ["bbr", "geofa", "osm"]
    assert kept["name"] == "GeoFA name"
    assert stats["bbr+geofa"] == 1
    assert stats["geofa+osm"] == 1
    assert "osm:node/4" in {row["receptor_id"] for row in output}


def test_wgs84_point_transform_matches_epsg_projection() -> None:
    connection = duckdb.connect()
    connection.execute("LOAD spatial")
    east, north = connection.execute(
        "SELECT ST_X(ST_Transform(ST_Point(12.5683, 55.6761), 'EPSG:4326', 'EPSG:25832', true)), "
        "ST_Y(ST_Transform(ST_Point(12.5683, 55.6761), 'EPSG:4326', 'EPSG:25832', true))"
    ).fetchone()
    assert east == pytest.approx(724351.93, abs=1)
    assert north == pytest.approx(6175804.02, abs=1)


def test_write_parquet_has_geometry_column_and_epsg_25832_metadata(tmp_path: Path) -> None:
    row = _row("geofa:1", "geofa", 500000)
    destination = write_parquet([row], tmp_path / "data.parquet")
    parquet_file = pq.ParquetFile(destination)
    metadata = parquet_file.schema_arrow.metadata or {}
    geo_metadata = json.loads(metadata[b"geo"])
    assert geo_metadata["columns"]["geometry"]["crs"]["id"]["authority"] == "EPSG"
    assert geo_metadata["columns"]["geometry"]["crs"]["id"]["code"] == 25832
    connection = duckdb.connect()
    connection.execute("LOAD spatial")
    values = connection.execute(
        "SELECT receptor_id, ST_AsText(geometry) FROM read_parquet(?)",
        [str(destination)],
    ).fetchone()
    assert values[0] == "geofa:1"
    assert values[1] == "POINT (500000 6170000)"


def test_build_silver_reads_local_bronze_and_uses_manifest_metadata(fixtures_dir: Path, tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze" / "child_receptors"
    run = bronze_root / "bbr" / "20261010_120000"
    run.mkdir(parents=True)
    (run / "bbr_tekniskanlaeg_1905.jsonl").write_bytes((fixtures_dir / "bbr_1905.sample.json").read_bytes())
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "_source": "https://graphql.datafordeler.dk/BBR/v2",
                "_fetch_timestamp": "2026-10-10T12:00:00Z",
                "_source_crs": "EPSG:25832",
                "files": {"bbr_tekniskanlaeg_1905.jsonl": {"bytes": 10, "rows": 6}},
            }
        ),
        encoding="utf-8",
    )

    rows, qa = build_silver(bronze_root, sources=("bbr",), enforce_floors=False)
    assert len(rows) == 4
    assert all(row["_fetch_timestamp"] == "2026-10-10T12:00:00Z" for row in rows)
    assert all(row["_source_crs"] == "EPSG:25832" for row in rows)
    assert qa["source_runs"] == {"bbr": "20261010_120000"}
    assert qa["row_count"] == 4
    assert qa["counts_by_source_receptor_type_kommune"]["bbr"]["playground"]["0147"] == 4


def _write_osm_run(run_dir: Path, *, region_count: int, include_manifest: bool = True) -> None:
    run_dir.mkdir(parents=True)
    for index, filename in enumerate(SOURCE_FILENAMES["osm"][:region_count]):
        payload = {
            "elements": [
                {
                    "type": "node",
                    "id": index + 1,
                    "lat": 55.6761,
                    "lon": 12.5683,
                    "tags": {"leisure": "playground"},
                }
            ]
        }
        (run_dir / filename).write_text(json.dumps(payload), encoding="utf-8")
    if include_manifest:
        (run_dir / "manifest.json").write_text(
            json.dumps({"_fetch_timestamp": f"{run_dir.name}:fetch", "_source_crs": "EPSG:4326"}),
            encoding="utf-8",
        )


def test_build_silver_uses_latest_complete_earlier_osm_run(tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze" / "child_receptors"
    _write_osm_run(bronze_root / "osm" / "20261009_120000", region_count=5, include_manifest=False)
    _write_osm_run(bronze_root / "osm" / "20261010_120000", region_count=2)

    rows, qa = build_silver(bronze_root, sources=("osm",), enforce_floors=False)

    assert qa["osm"] == {
        "status": "stale",
        "bronze_timestamp": "20261009_120000",
        "regions": list(OSM_REGIONS),
    }
    assert qa["source_runs"]["osm"] == "20261009_120000"
    assert len(rows) == 1


def test_missing_osm_uses_9000_playground_floor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze" / "child_receptors"
    bbr_run = bronze_root / "bbr" / "20261010_120000"
    bbr_run.mkdir(parents=True)
    (bbr_run / "bbr_tekniskanlaeg_1905.jsonl").write_text("", encoding="utf-8")
    (bbr_run / "manifest.json").write_text(
        json.dumps({"_fetch_timestamp": "2026-10-10T12:00:00Z", "_source_crs": "EPSG:25832"}),
        encoding="utf-8",
    )
    playgrounds = [_row(f"bbr:{index}", "bbr", 401000 + index * 50) for index in range(9000)]
    monkeypatch.setattr(silver_build, "parse_bbr", lambda *_args, **_kwargs: playgrounds)
    monkeypatch.setattr(
        silver_build,
        "PRODUCTION_FLOORS",
        {"daycare": 0, "school": 0, "playground": 10000, "daycare_kommune_count": 0},
    )

    rows, qa = build_silver(bronze_root, sources=("bbr", "osm"))

    assert len(rows) == 9000
    assert qa["osm"] == {"status": "missing", "bronze_timestamp": None, "regions": []}
