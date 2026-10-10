"""No-network CLI orchestration checks for the child receptors pipeline."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from loguru import logger

from child_receptors.config import OSM_REGIONS, SOURCE_FILENAMES
from child_receptors.silver.build import build_silver

MAIN_PATH = Path(__file__).parents[1] / "main.py"
MAIN_SPEC = importlib.util.spec_from_file_location("child_receptors_cli", MAIN_PATH)
assert MAIN_SPEC and MAIN_SPEC.loader
cli = importlib.util.module_from_spec(MAIN_SPEC)
sys.modules[MAIN_SPEC.name] = cli
MAIN_SPEC.loader.exec_module(cli)


def _csv_bytes(headers: list[str], rows: list[dict[str, str]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=headers, delimiter=";", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8-sig")


def _fixture_fetches() -> dict[str, dict[str, bytes]]:
    daginstitution = _csv_bytes(
        ["daginstitutionsNummer", "ejerformKode", "ejerformKode_Tekst"],
        [{"daginstitutionsNummer": "D1", "ejerformKode": "1", "ejerformKode_Tekst": "Kommunal"}],
    )
    anvisningsenhed = _csv_bytes(
        [
            "anvisningsenhedsNummer",
            "daginstitutionsNummer",
            "instType3",
            "aktivitetsstatus",
            "navn",
            "kommuneKode",
            "geoBredde",
            "geoLaengde",
        ],
        [
            {
                "anvisningsenhedsNummer": "A1",
                "daginstitutionsNummer": "D1",
                "instType3": "6010",
                "aktivitetsstatus": "Aktiv",
                "navn": "Børnehuset",
                "kommuneKode": "101",
                "geoBredde": "55.6761",
                "geoLaengde": "12.5683",
            }
        ],
    )
    alternative = _csv_bytes(["alternativAdresseNummer", "anvisningsenhedsNummer", "geoBredde", "geoLaengde"], [])
    samlet = _csv_bytes(["dagtilbudsType"], [{"dagtilbudsType": "Dagtilbud"}])
    stil = json.dumps(
        [
            {
                "activeCodeName": "Aktiv juridisk institution",
                "institutionTypeId": 1012,
                "institutionNumber": "S1",
                "institutionType": "Folkeskoler",
                "institutionName": "Skolen",
                "locationMunicipality": "Københavns Kommune",
                "longitude": 12.5683,
                "latitude": 55.6761,
            }
        ]
    ).encode()
    bbr = json.dumps(
        [
            {
                "id_lokalId": "bbr-1",
                "kommunekode": "0101",
                "status": "6",
                "registreringFra": "2026-01-01T00:00:00Z",
                "virkningFra": "2026-01-01T00:00:00Z",
                "tek109Koordinat": {"wkt": "POINT (724300 6175800)"},
            }
        ]
    ).encode()
    geofa = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "objekt_id": "g1",
                        "kommunekode": "0101",
                        "facil_ty_k": 1031,
                        "facil_ty": "Legeplads",
                        "statuskode": 3,
                        "navn": "Legeplads",
                    },
                    "geometry": {"type": "MultiPoint", "coordinates": [[724300, 6175800]]},
                }
            ],
        }
    ).encode()
    osm = {
        filename: json.dumps(
            {
                "elements": [
                    {
                        "type": "node",
                        "id": index + 1,
                        "lat": 55.6761,
                        "lon": 12.5683,
                        "tags": {"leisure": "playground", "name": f"OSM {index + 1}"},
                    }
                ]
            }
        ).encode()
        for index, filename in enumerate(SOURCE_FILENAMES["osm"])
    }
    return {
        "dagtilbud": dict(
            zip(SOURCE_FILENAMES["dagtilbud"], (daginstitution, anvisningsenhed, alternative, samlet), strict=True)
        ),
        "stil": {SOURCE_FILENAMES["stil"][0]: stil},
        "bbr": {SOURCE_FILENAMES["bbr"][0]: bbr},
        "geofa": {SOURCE_FILENAMES["geofa"][0]: geofa},
        "osm": osm,
    }


def _patch_fetchers(monkeypatch: pytest.MonkeyPatch, fetches: dict[str, dict[str, bytes]]) -> None:
    for source, files in fetches.items():
        monkeypatch.setitem(cli.FETCHERS, source, lambda files=files: files)


def _patch_small_floors(monkeypatch: pytest.MonkeyPatch) -> None:
    def build_with_small_floors(bronze_root: Path, **kwargs):
        return build_silver(
            bronze_root,
            minimums={"daycare": 0, "school": 0, "playground": 0, "daycare_kommune_count": 0},
            **kwargs,
        )

    monkeypatch.setattr(cli, "build_silver", build_with_small_floors)


def _qa_path(local_dir: Path) -> Path:
    silver_root = local_dir / "silver" / "child_receptors"
    return next(silver_root.iterdir()) / "qa_report.json"


def test_cli_all_writes_bronze_manifests_and_silver_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fetches = _fixture_fetches()
    _patch_fetchers(monkeypatch, fetches)
    _patch_small_floors(monkeypatch)
    monkeypatch.setattr(cli, "timestamp_now", lambda: "20261010_120000")

    assert cli.main(["--layer", "all", "--no-upload", "--local-dir", str(tmp_path)]) == 0

    bronze_root = tmp_path / "bronze" / "child_receptors"
    for source, files in fetches.items():
        run_dir = bronze_root / source / "20261010_120000"
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        assert set(manifest["files"]) == set(files)
        assert all(entry["rows"] >= 0 for entry in manifest["files"].values())
    dagtilbud_manifest = json.loads(
        (bronze_root / "dagtilbud" / "20261010_120000" / "manifest.json").read_text(encoding="utf-8")
    )
    assert dagtilbud_manifest["files"][SOURCE_FILENAMES["dagtilbud"][0]]["rows"] == 1
    assert dagtilbud_manifest["files"][SOURCE_FILENAMES["dagtilbud"][2]]["rows"] == 0

    silver_dir = _qa_path(tmp_path).parent
    assert (silver_dir / "data.parquet").is_file()
    parquet_columns = set(pq.ParquetFile(silver_dir / "data.parquet").schema_arrow.names)
    assert {
        "receptor_id",
        "receptor_type",
        "subtype",
        "name",
        "ownership",
        "kommune_kode",
        "address",
        "dawa_id",
        "cvr",
        "p_number",
        "is_active",
        "source",
        "matched_sources",
        "source_updated_at",
        "utm_e",
        "utm_n",
        "lon",
        "lat",
        "geometry",
        "_fetch_timestamp",
        "_source_crs",
    } <= parquet_columns
    qa = json.loads(_qa_path(tmp_path).read_text(encoding="utf-8"))
    assert qa["osm"] == {"status": "fresh", "bronze_timestamp": "20261010_120000", "regions": list(OSM_REGIONS)}


def test_cli_continues_when_osm_fails_and_reports_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fetches = _fixture_fetches()
    _patch_fetchers(monkeypatch, fetches)

    def fail_osm() -> dict[str, bytes]:
        raise RuntimeError("all Overpass endpoints timed out")

    monkeypatch.setitem(cli.FETCHERS, "osm", fail_osm)
    _patch_small_floors(monkeypatch)
    monkeypatch.setattr(cli, "timestamp_now", lambda: "20261010_120000")
    warning_stream = io.StringIO()
    sink_id = logger.add(warning_stream, format="{level} {message}", level="WARNING")
    try:
        assert cli.main(["--layer", "all", "--no-upload", "--local-dir", str(tmp_path)]) == 0
    finally:
        logger.remove(sink_id)

    assert all(
        (tmp_path / "bronze" / "child_receptors" / source / "20261010_120000" / "manifest.json").is_file()
        for source in ("dagtilbud", "stil", "bbr", "geofa")
    )
    assert not (tmp_path / "bronze" / "child_receptors" / "osm" / "20261010_120000").exists()
    qa = json.loads(_qa_path(tmp_path).read_text(encoding="utf-8"))
    assert qa["osm"] == {"status": "missing", "bronze_timestamp": None, "regions": []}
    assert "WARNING OSM fetch failed; skipping OSM bronze for this run" in warning_stream.getvalue()
    assert "Traceback" not in warning_stream.getvalue()
