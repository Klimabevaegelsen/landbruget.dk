"""Tests for DAGI silver processing and its WGS84 compatibility input."""

import json

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
