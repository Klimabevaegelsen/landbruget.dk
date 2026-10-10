"""Tests for DAGI silver processing and its WGS84 compatibility input."""

import json
from unittest.mock import MagicMock

import duckdb
import pytest
from loguru import logger
from pyproj import Transformer
from shapely.geometry import Polygon, mapping
from shapely.ops import transform

from unified_pipeline.silver.dagi import DAGISilver, DAGISilverConfig


def test_area_is_square_metres_and_centroid_stays_wgs84() -> None:
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    polygon_utm = Polygon(
        [(724000, 6175500), (725000, 6175500), (725000, 6176500), (724000, 6176500)]
    )
    polygon_wgs84 = transform(transformer.transform, polygon_utm)
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"kode": "0101", "navn": "Testkommune", "regionskode": "1084"},
                "geometry": mapping(polygon_wgs84),
            }
        ],
    }
    silver = DAGISilver.__new__(DAGISilver)
    silver.config = DAGISilverConfig()
    silver.conn = duckdb.connect()
    silver.conn.execute("LOAD spatial")
    silver.log = logger

    table = silver._process_layer(json.dumps(geojson), "kommuner")

    assert table is not None
    area_m2, centroid_x, centroid_y = silver.conn.execute(
        f"SELECT area_m2, centroid_x, centroid_y FROM {table}"
    ).fetchone()
    assert area_m2 == pytest.approx(1_000_000, rel=0.02)
    expected_centroid = transform(transformer.transform, polygon_utm.centroid)
    assert centroid_x == pytest.approx(expected_centroid.x, abs=1e-5)
    assert centroid_y == pytest.approx(expected_centroid.y, abs=1e-5)


def test_sea_inclusive_postnummer_is_transformed_to_utm() -> None:
    """Postnumre reach lon ~3.2 in the North Sea, outside bounds-based CRS detection."""
    to_wgs84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    polygon_utm = Polygon(
        [(150000, 6400000), (151000, 6400000), (151000, 6401000), (150000, 6401000)]
    )
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"nr": "6990", "navn": "Ulfborg"},
                "geometry": mapping(transform(to_wgs84.transform, polygon_utm)),
            }
        ],
    }
    silver = DAGISilver.__new__(DAGISilver)
    silver.config = DAGISilverConfig()
    silver.conn = duckdb.connect()
    silver.conn.execute("LOAD spatial")
    silver.log = logger

    table = silver._process_layer(json.dumps(geojson), "postnumre")

    assert table is not None
    min_x, area_m2 = silver.conn.execute(
        f"SELECT ST_XMin(geometry), area_m2 FROM {table}"
    ).fetchone()
    assert min_x == pytest.approx(150000, abs=1)
    assert area_m2 == pytest.approx(1_000_000, rel=0.01)


def _silver_for_storage() -> DAGISilver:
    silver = DAGISilver.__new__(DAGISilver)
    silver.config = DAGISilverConfig()
    silver.storage = MagicMock()
    silver.log = logger
    silver.date_pattern = "20261010_120000"
    return silver


def _snapshot_manifest(snapshot_id: str, dataset: str = "dagi") -> dict:
    return {
        "snapshot_id": snapshot_id,
        "complete": True,
        "layers": {
            layer: (
                f"landbruget-data/bronze/{dataset}_{layer}/{snapshot_id}/{dataset}_{layer}.json"
            )
            for layer in DAGISilverConfig.endpoints
        },
    }


def test_storage_read_skips_incomplete_newer_and_uses_one_completed_snapshot() -> None:
    silver = _silver_for_storage()
    newer_path = "landbruget-data/bronze/dagi/20261010_120001/completion.json"
    older_path = "landbruget-data/bronze/dagi/20261010_120000/completion.json"
    newer = _snapshot_manifest("20261010_120001")
    del newer["layers"]["regioner"]
    older = _snapshot_manifest("20261010_120000")
    payloads = {newer_path: newer, older_path: older}
    payloads.update(
        {
            layer_path: {"type": "FeatureCollection", "snapshot": "older"}
            for layer_path in older["layers"].values()
        }
    )
    silver.storage.list_files.return_value = [newer_path, older_path]
    silver.storage.download_json.side_effect = payloads.__getitem__

    loaded = silver._load_bronze_snapshot()

    assert set(loaded) == set(DAGISilverConfig.endpoints)
    assert all(json.loads(value)["snapshot"] == "older" for value in loaded.values())
    assert all(
        path in [call.args[0] for call in silver.storage.download_json.call_args_list]
        for path in older["layers"].values()
    )


def test_storage_read_loads_a_successful_complete_snapshot() -> None:
    silver = _silver_for_storage()
    manifest_path = "landbruget-data/bronze/dagi/20261010_120000/completion.json"
    manifest = _snapshot_manifest("20261010_120000")
    payloads = {manifest_path: manifest}
    payloads.update(
        {
            layer_path: {"type": "FeatureCollection", "features": []}
            for layer_path in manifest["layers"].values()
        }
    )
    silver.storage.list_files.return_value = [manifest_path]
    silver.storage.download_json.side_effect = payloads.__getitem__

    loaded = silver._load_bronze_snapshot()

    assert set(loaded) == set(DAGISilverConfig.endpoints)
    assert all(json.loads(value)["type"] == "FeatureCollection" for value in loaded.values())


def test_storage_read_rejects_layer_from_mixed_snapshot() -> None:
    silver = _silver_for_storage()
    manifest_path = "landbruget-data/bronze/dagi/20261010_120000/completion.json"
    manifest = _snapshot_manifest("20261010_120000")
    manifest["layers"]["postnumre"] = (
        "landbruget-data/bronze/dagi_postnumre/20261010_120001/dagi_postnumre.json"
    )
    silver.storage.list_files.return_value = [manifest_path]
    silver.storage.download_json.return_value = manifest

    with pytest.raises(FileNotFoundError, match="No valid completed DAGI bronze snapshot"):
        silver._load_bronze_snapshot()

    downloaded_paths = [call.args[0] for call in silver.storage.download_json.call_args_list]
    assert manifest["layers"]["postnumre"] not in downloaded_paths


def test_storage_read_rejects_snapshot_id_mismatched_with_manifest_directory() -> None:
    silver = _silver_for_storage()
    manifest_path = "landbruget-data/bronze/dagi/20261010_120000/completion.json"
    silver.storage.list_files.return_value = [manifest_path]
    silver.storage.download_json.return_value = _snapshot_manifest("20261010_120001")

    with pytest.raises(FileNotFoundError, match="No valid completed DAGI bronze snapshot"):
        silver._load_bronze_snapshot()

    assert silver.storage.download_json.call_count == 1


def test_markerless_storage_raises_actionable_no_snapshot_error() -> None:
    silver = _silver_for_storage()
    silver.storage.list_files.return_value = []

    with pytest.raises(FileNotFoundError, match="Markerless legacy files are not accepted"):
        silver._load_bronze_snapshot()


def test_dagi_silver_write_declares_epsg_25832_crs() -> None:
    silver = _silver_for_storage()

    path = silver._save_dagi_layer("processed_postnumre", "dagi_postnumre")

    assert path.endswith("/silver/dagi_postnumre/20261010_120000/data.parquet")
    silver.storage.upload_from_duckdb_table.assert_called_once_with(
        "processed_postnumre",
        path,
        compression="zstd",
        row_group_size=100000,
        crs="EPSG:25832",
    )


@pytest.mark.asyncio
async def test_in_memory_bronze_handoff_processes_all_configured_layers() -> None:
    silver = _silver_for_storage()
    silver._process_layer = MagicMock(
        side_effect=lambda _raw, layer_name: f"processed_{layer_name}"
    )
    silver._save_dagi_layer = MagicMock(side_effect=lambda _table, dataset: dataset)
    bronze_data = dict.fromkeys(
        DAGISilverConfig.endpoints, '{"type":"FeatureCollection","features":[]}'
    )

    processed = await silver.run(bronze_data)

    assert set(processed) == set(DAGISilverConfig.endpoints)
    assert silver._process_layer.call_count == len(DAGISilverConfig.endpoints)
    silver.storage.list_files.assert_not_called()
