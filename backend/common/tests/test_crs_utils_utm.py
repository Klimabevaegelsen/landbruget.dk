"""Tests for pure Python Danish UTM coordinate conversion."""

import pytest
from common.crs_utils import utm32_to_wgs84
from pyproj import Transformer


@pytest.mark.parametrize(
    "easting,northing",
    [
        (724434.93, 6175755.61),
        (577030.0, 6224098.0),
        (500000.0, 6200000.0),
        (750000.0, 6300000.0),
        (400000.0, 6400000.0),
    ],
)
def test_utm32_to_wgs84_matches_pyproj(easting, northing):
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    expected_lon, expected_lat = transformer.transform(easting, northing)

    longitude, latitude = utm32_to_wgs84(easting, northing)

    assert longitude == pytest.approx(expected_lon, abs=1e-7)
    assert latitude == pytest.approx(expected_lat, abs=1e-7)


def test_copenhagen_reference_point():
    longitude, latitude = utm32_to_wgs84(724434.93, 6175755.61)

    assert latitude == pytest.approx(55.6757, abs=0.0001)
    assert longitude == pytest.approx(12.5690, abs=0.001)
