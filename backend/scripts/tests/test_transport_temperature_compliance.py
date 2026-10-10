"""Targeted tests for transport report spatial setup and geocoding inputs."""

import json
from unittest.mock import MagicMock

import duckdb
import pytest
from loguru import logger
from pyproj import Transformer
from shapely.geometry import box, mapping
from shapely.ops import transform
from unified_pipeline.bronze.dagi import DAGIBronze
from unified_pipeline.silver.dagi import DAGISilver, DAGISilverConfig

from scripts import transport_temperature_compliance as transport


def test_sea_inclusive_dagi_geometry_flows_bronze_to_silver_to_transport(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    polygon_utm = box(150000, 6400000, 151000, 6401000)
    to_wgs84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
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

    DAGIBronze._validate_geojson("postnumre", geojson)

    silver = DAGISilver.__new__(DAGISilver)
    silver.config = DAGISilverConfig()
    silver.conn = duckdb.connect()
    silver.conn.execute("INSTALL spatial")
    silver.conn.execute("LOAD spatial")
    silver.log = logger
    table = silver._process_layer(json.dumps(geojson), "postnumre")
    assert table is not None
    min_x, area_m2 = silver.conn.execute(f"SELECT ST_XMin(geometry), area_m2 FROM {table}").fetchone()
    assert min_x == pytest.approx(150000, abs=1)
    assert area_m2 == pytest.approx(1_000_000, rel=0.01)

    parquet_path = tmp_path / "dagi_postnumre.parquet"
    silver.conn.execute(f"COPY {table} TO '{parquet_path}' (FORMAT PARQUET)")
    reader = duckdb.connect()
    monkeypatch.setattr(transport, "_latest_postal_code_parquet", lambda _conn: str(parquet_path))

    coordinates = transport._read_postal_code_centroids(reader, [6990])

    expected_lon, expected_lat = to_wgs84.transform(150500, 6400500)
    assert coordinates[6990] == pytest.approx((expected_lat, expected_lon), abs=1e-5)
    reader.close()
    silver.conn.close()


def test_owned_connection_loads_spatial_and_closes_after_postcode_read(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parquet_path = tmp_path / "dagi_postnumre.parquet"
    writer = duckdb.connect()
    writer.execute("INSTALL spatial")
    writer.execute("LOAD spatial")
    writer.execute(
        "CREATE TABLE postnumre AS SELECT '1000' AS code, ST_GeomFromText('POLYGON((700000 6170000, 701000 6170000, 701000 6171000, 700000 6171000, 700000 6170000))') AS geometry"
    )
    writer.execute(f"COPY postnumre TO '{parquet_path}' (FORMAT PARQUET)")
    writer.close()

    connection = duckdb.connect()
    monkeypatch.setattr(transport.duckdb, "connect", lambda: connection)
    monkeypatch.setattr(transport, "setup_duckdb_cloud_auth", MagicMock())
    monkeypatch.setattr(
        transport,
        "_latest_postal_code_parquet",
        lambda _conn: str(parquet_path),
    )

    result = transport.geocode_postal_codes([1000], tmp_path / "coords.json")

    expected_lon, expected_lat = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True).transform(
        700500, 6170500
    )
    assert result[1000] == pytest.approx((expected_lat, expected_lon), abs=1e-5)
    with pytest.raises(duckdb.ConnectionException):
        connection.execute("SELECT 1")


def test_incomplete_postcode_cache_fails_without_adressevaelger_search(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = MagicMock()
    monkeypatch.setattr(transport, "_read_postal_code_centroids", lambda _conn, _codes: {})
    adressevaelger = MagicMock()
    monkeypatch.setattr(transport, "AdressevaelgerClient", lambda: adressevaelger)

    with pytest.raises(RuntimeError, match="cache is incomplete"):
        transport.geocode_postal_codes([6990], tmp_path / "coords.json", conn=connection)

    adressevaelger.search_address.assert_not_called()
    adressevaelger.geocode_free_text.assert_not_called()


def test_address_geocoding_uses_shared_client_with_postcode(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE cold_movements (
            sender_address VARCHAR,
            sender_postal_code INTEGER,
            receiver_address VARCHAR,
            receiver_postal_code INTEGER
        )
    """)
    conn.execute("INSERT INTO cold_movements VALUES ('Nørregade 1', 6000, NULL, NULL)")
    client = MagicMock()
    client.geocode_free_text.return_value = {"latitude": 55.5, "longitude": 9.5}
    monkeypatch.setattr(transport, "AdressevaelgerClient", lambda: client)

    coords = transport.geocode_addresses(conn, tmp_path / "addresses.json")

    assert coords == {"Nørregade 1|6000": (55.5, 9.5)}
    client.geocode_free_text.assert_called_once_with("Nørregade 1", "6000")
    assert json.loads((tmp_path / "addresses.json").read_text()) == {"Nørregade 1|6000": [55.5, 9.5]}
    conn.close()
