"""Coverage limits for DAGI geometry, including its sea-inclusive postcodes."""

from __future__ import annotations

import math
from typing import Literal

DAGI_COVERAGE_BOUNDS = {
    "EPSG:4326": (2.5, 53.0, 18.0, 59.0),
    "EPSG:25832": (50_000.0, 5_850_000.0, 1_000_000.0, 6_600_000.0),
}


def validate_dagi_coverage(
    bounds: tuple[float, float, float, float],
    crs: Literal["EPSG:4326", "EPSG:25832"],
    *,
    layer_name: str,
) -> None:
    """Reject DAGI geometries outside broad explicit Denmark coverage bounds.

    Bounds are ordered ``(min_x, min_y, max_x, max_y)``. The coverage includes
    the North Sea postcodes while remaining narrow enough to reject clearly
    unrelated geographies.
    """
    min_x, min_y, max_x, max_y = bounds
    if not all(math.isfinite(value) for value in bounds):
        raise ValueError(f"DAGI {layer_name} geometry has non-finite {crs} bounds: {bounds}")
    if min_x > max_x or min_y > max_y:
        raise ValueError(f"DAGI {layer_name} geometry has inverted {crs} bounds: {bounds}")

    coverage = DAGI_COVERAGE_BOUNDS[crs]
    min_allowed_x, min_allowed_y, max_allowed_x, max_allowed_y = coverage
    if min_x < min_allowed_x or min_y < min_allowed_y or max_x > max_allowed_x or max_y > max_allowed_y:
        raise ValueError(f"DAGI {layer_name} geometry is outside declared {crs} coverage: {bounds}")
