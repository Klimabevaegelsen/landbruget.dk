"""Tests for DAGI coverage bounds validation."""

import math

import pytest
from common.dagi_coverage import validate_dagi_coverage


@pytest.mark.parametrize(
    "bounds",
    [
        (math.nan, 55.0, 12.0, 56.0),
        (7.0, 55.0, math.inf, 56.0),
        (8.0, 55.0, 7.0, 56.0),
        (8.0, 56.0, 9.0, 55.0),
    ],
)
def test_rejects_nonfinite_or_inverted_bounds(bounds: tuple[float, float, float, float]) -> None:
    with pytest.raises(ValueError):
        validate_dagi_coverage(bounds, "EPSG:4326", layer_name="postnumre")
