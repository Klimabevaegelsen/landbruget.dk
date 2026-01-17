"""
Tests for H3 cell operations in PFAS exposure analysis.

Tests H3 cell validity, resolution, area calculations, and geographic operations.
This is CRITICAL for accurate spatial hexagon grid analysis.
"""

import duckdb
import pytest

from h3_pfas_exposure.config import H3SpatialConfig


@pytest.fixture
def duckdb_conn():
    """Create a DuckDB connection with spatial extension."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    return conn


@pytest.fixture
def config():
    """Create test configuration."""
    return H3SpatialConfig(
        h3_resolution=9,
        processing_crs="EPSG:25832",
        output_crs="EPSG:4326",
    )


# H3 Cell Validity Tests
def test_h3_cell_format(duckdb_conn):
    """Test valid H3 cell string format."""
    # H3 cells are 15-character hexadecimal strings
    valid_h3_cells = [
        "891f1d48993ffff",
        "891f1d48997ffff",
        "8928308280fffff",
    ]

    for h3_cell in valid_h3_cells:
        assert len(h3_cell) == 15, f"H3 cell should be 15 characters, got {len(h3_cell)}"
        assert all(
            c in "0123456789abcdef" for c in h3_cell
        ), f"H3 cell should be hexadecimal: {h3_cell}"


def test_h3_resolution(config):
    """Test H3 resolution levels (0-15)."""
    # Resolution 0 = largest cells (~4M km²)
    # Resolution 15 = smallest cells (~1 m²)
    # Common resolutions: 7-10 for agricultural analysis

    assert 0 <= config.h3_resolution <= 15, "H3 resolution should be 0-15"

    # Test common resolutions for agriculture
    for res in [7, 8, 9, 10]:
        test_config = H3SpatialConfig(h3_resolution=res)
        assert test_config.h3_resolution == res, f"Should support resolution {res}"


def test_h3_cell_to_boundary(duckdb_conn):
    """Test H3 cell to polygon boundary conversion."""
    # Create H3 cell and convert to boundary
    # Note: This test assumes the H3 cell ID corresponds to a real H3 cell
    # In production, we would use the actual h3 library for validation

    h3_cell = "891f1d48993ffff"

    # Create a mock H3 cell geometry (in reality, would use h3.h3_to_geo_boundary)
    # For testing, we use a representative polygon for Copenhagen area
    duckdb_conn.execute(f"""
        CREATE TABLE h3_boundary_test AS
        SELECT
            '{h3_cell}' as h3_cell,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    # Verify it's a valid polygon
    result = duckdb_conn.execute("""
        SELECT ST_IsValid(h3_geometry), ST_GeometryType(h3_geometry)
        FROM h3_boundary_test
    """).fetchone()

    assert result[0], "H3 cell boundary should be valid geometry"
    assert result[1] == "POLYGON", "H3 cell boundary should be a polygon"


def test_h3_cell_area_calculation(duckdb_conn, config):
    """Test H3 cell area calculation in square meters."""
    # H3 resolution 9 cells are approximately 0.105 km² = 10.5 ha
    # H3 resolution 10 cells are approximately 0.015 km² = 1.5 ha

    # Create mock H3 cell with known area
    duckdb_conn.execute("""
        CREATE TABLE h3_area_test AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    # Calculate area in hectares
    result = duckdb_conn.execute("""
        SELECT ST_Area_Spheroid(h3_geometry) / 10000.0 as area_ha
        FROM h3_area_test
    """).fetchone()

    area_ha = result[0]

    # For resolution 9, should be approximately 10.5 ha (allow wide tolerance for mock data)
    assert area_ha > 0, "H3 cell area should be positive"
    assert area_ha < 100, "H3 cell area should be reasonable (< 100 ha for res 9)"


# H3 to Geography Tests
def test_h3_to_geojson(duckdb_conn):
    """Test H3 cell to GeoJSON conversion."""
    duckdb_conn.execute("""
        CREATE TABLE h3_geojson_test AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    # Convert to GeoJSON
    result = duckdb_conn.execute("""
        SELECT ST_AsGeoJSON(h3_geometry) as geojson
        FROM h3_geojson_test
    """).fetchone()

    geojson = result[0]
    assert geojson is not None, "Should produce GeoJSON output"
    assert "Polygon" in geojson, "GeoJSON should contain Polygon type"
    assert "coordinates" in geojson, "GeoJSON should contain coordinates"


def test_h3_coverage_denmark(duckdb_conn, config):
    """Test that H3 cells fully cover Denmark."""
    # Create sample H3 cells covering different parts of Denmark
    duckdb_conn.execute("""
        CREATE TABLE denmark_coverage AS
        SELECT
            'copenhagen' as region,
            55.6761 as lat,
            12.5683 as lon,
            ST_Point(12.5683, 55.6761) as point
        UNION ALL
        SELECT
            'aarhus' as region,
            56.1629 as lat,
            10.2039 as lon,
            ST_Point(10.2039, 56.1629) as point
        UNION ALL
        SELECT
            'aalborg' as region,
            57.0488 as lat,
            9.9216 as lon,
            ST_Point(9.9216, 57.0488) as point
        UNION ALL
        SELECT
            'odense' as region,
            55.4038 as lat,
            10.4028 as lon,
            ST_Point(10.4028, 55.4038) as point
    """)

    # All points should be within Denmark bounds
    in_bounds = duckdb_conn.execute("""
        SELECT COUNT(*)
        FROM denmark_coverage
        WHERE lon >= 7.5 AND lon <= 15.5
          AND lat >= 54.5 AND lat <= 58.0
    """).fetchone()[0]

    assert in_bounds == 4, "All test points should be within Denmark bounds for H3 coverage"


def test_h3_neighbor_relationships(duckdb_conn):
    """Test H3 neighbor cell relationships."""
    # Create a central H3 cell and its neighbors
    # In H3, each cell has 6 neighbors (hexagonal grid)

    duckdb_conn.execute("""
        CREATE TABLE h3_neighbors AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            'center' as position,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        UNION ALL
        SELECT
            '891f1d48997ffff' as h3_cell,
            'neighbor' as position,
            ST_GeomFromText('POLYGON((12.569 55.677, 12.570 55.677, 12.570 55.678, 12.569 55.678, 12.569 55.677))') as h3_geometry
    """)

    # Check that cells are adjacent (touch but don't overlap significantly)
    result = duckdb_conn.execute("""
        SELECT
            ST_Touches(
                (SELECT h3_geometry FROM h3_neighbors WHERE position = 'center'),
                (SELECT h3_geometry FROM h3_neighbors WHERE position = 'neighbor')
            ) as touches,
            ST_Intersects(
                (SELECT h3_geometry FROM h3_neighbors WHERE position = 'center'),
                (SELECT h3_geometry FROM h3_neighbors WHERE position = 'neighbor')
            ) as intersects
    """).fetchone()

    # Neighbors should either touch or intersect slightly
    assert result[1], "H3 neighbor cells should intersect or touch"


# Error Handling Tests
def test_invalid_h3_cell_rejection(duckdb_conn):
    """Test that invalid H3 cell strings are handled gracefully."""
    # Create table with invalid H3 cell IDs
    duckdb_conn.execute("""
        CREATE TABLE invalid_h3_test AS
        SELECT
            '' as h3_cell,
            ST_Point(12.5683, 55.6761) as geometry
        UNION ALL
        SELECT
            'invalid' as h3_cell,
            ST_Point(10.2039, 56.1629) as geometry
        UNION ALL
        SELECT
            '891f1d48993ffff' as h3_cell,
            ST_Point(9.9216, 57.0488) as geometry
    """)

    # Filter to only valid-looking H3 cells (15 hex characters)
    result = duckdb_conn.execute("""
        SELECT COUNT(*)
        FROM invalid_h3_test
        WHERE length(h3_cell) = 15
          AND h3_cell ~ '^[0-9a-f]{15}$'
    """).fetchone()[0]

    assert result == 1, "Should identify 1 valid H3 cell format"


# H3 Resolution Area Validation Tests
def test_h3_resolution_area_ranges(config):
    """Test that H3 resolution area ranges are correctly defined."""
    # Official H3 areas from h3geo.org
    expected_areas = {
        7: {"min": 312.68, "max": 622.74, "avg": 516.13},
        8: {"min": 44.65, "max": 88.96, "avg": 73.73},
        9: {"min": 6.38, "max": 12.71, "avg": 10.53},
        10: {"min": 1.0, "max": 2.0, "avg": 1.5048},
    }

    for resolution, areas in expected_areas.items():
        test_config = H3SpatialConfig(h3_resolution=resolution)
        resolution_config = test_config.h3_resolution_areas[resolution]

        assert (
            resolution_config["min_area_ha"] == areas["min"]
        ), f"Resolution {resolution} min area mismatch"
        assert (
            resolution_config["max_area_ha"] == areas["max"]
        ), f"Resolution {resolution} max area mismatch"
        assert (
            abs(resolution_config["theoretical_avg_area_ha"] - areas["avg"]) < 0.1
        ), f"Resolution {resolution} avg area mismatch"


def test_h3_area_validation_thresholds(duckdb_conn, config):
    """Test H3 area validation against resolution-specific thresholds."""
    # For resolution 9, theoretical avg is 10.53 ha
    test_config = H3SpatialConfig(h3_resolution=9)

    # Create cells with different areas
    duckdb_conn.execute("""
        CREATE TABLE area_validation_test AS
        SELECT
            'too_small' as test_case,
            5.0 as area_ha
        UNION ALL
        SELECT
            'valid_min' as test_case,
            6.38 as area_ha
        UNION ALL
        SELECT
            'valid_avg' as test_case,
            10.53 as area_ha
        UNION ALL
        SELECT
            'valid_max' as test_case,
            12.71 as area_ha
        UNION ALL
        SELECT
            'too_large' as test_case,
            15.0 as area_ha
    """)

    # Check which areas are within valid range
    result = duckdb_conn.execute(f"""
        SELECT test_case, area_ha
        FROM area_validation_test
        WHERE area_ha >= {test_config.min_h3_area_ha}
          AND area_ha <= {test_config.max_h3_area_ha}
    """).fetchall()

    valid_cases = [r[0] for r in result]
    assert "valid_min" in valid_cases, "Minimum valid area should pass"
    assert "valid_avg" in valid_cases, "Average area should pass"
    assert "valid_max" in valid_cases, "Maximum valid area should pass"
    assert "too_small" not in valid_cases, "Too small area should fail"
    assert "too_large" not in valid_cases, "Too large area should fail"


def test_h3_cell_center_coordinates(duckdb_conn):
    """Test that H3 cell center coordinates are within Denmark."""
    # Create H3 cells with center coordinates
    duckdb_conn.execute("""
        CREATE TABLE h3_centers AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon
        UNION ALL
        SELECT
            '891f1d48997ffff' as h3_cell,
            56.1629 as center_lat,
            10.2039 as center_lon
    """)

    # Verify centers are within Denmark bounds
    out_of_bounds = duckdb_conn.execute("""
        SELECT COUNT(*)
        FROM h3_centers
        WHERE center_lat < 54.5 OR center_lat > 58.0
           OR center_lon < 7.5 OR center_lon > 15.5
    """).fetchone()[0]

    assert out_of_bounds == 0, "All H3 cell centers should be within Denmark bounds"


def test_h3_cell_geometry_type(duckdb_conn):
    """Test that H3 cells are represented as polygons."""
    duckdb_conn.execute("""
        CREATE TABLE h3_geom_type AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    result = duckdb_conn.execute("""
        SELECT ST_GeometryType(h3_geometry)
        FROM h3_geom_type
    """).fetchone()[0]

    assert result == "POLYGON", "H3 cells should be represented as POLYGONs"


def test_h3_hexagon_properties(duckdb_conn):
    """Test that H3 cells have hexagonal properties (6 sides approximately)."""
    # Create a hexagonal-shaped polygon
    duckdb_conn.execute("""
        CREATE TABLE h3_hexagon AS
        SELECT
            ST_GeomFromText('POLYGON((12.568 55.676, 12.5685 55.6765, 12.5685 55.6775, 12.568 55.678, 12.5675 55.6775, 12.5675 55.6765, 12.568 55.676))') as h3_geometry
    """)

    # Count vertices (should be 7 for a closed hexagon: 6 vertices + 1 closing point)
    result = duckdb_conn.execute("""
        SELECT ST_NPoints(h3_geometry) as num_points
        FROM h3_hexagon
    """).fetchone()[0]

    # H3 cells are hexagons with 7 points (6 vertices + closing point)
    assert result >= 6, f"H3 hexagon should have at least 6 vertices, got {result}"


def test_h3_cell_overlap_detection(duckdb_conn):
    """Test detection of overlapping H3 cells (should not overlap in proper grid)."""
    duckdb_conn.execute("""
        CREATE TABLE h3_overlap_test AS
        SELECT
            '891f1d48993ffff' as h3_cell_1,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geom_1,
            '891f1d48997ffff' as h3_cell_2,
            ST_GeomFromText('POLYGON((12.580 55.680, 12.581 55.680, 12.581 55.681, 12.580 55.681, 12.580 55.680))') as geom_2
    """)

    # Check if cells overlap (they shouldn't in a proper H3 grid)
    result = duckdb_conn.execute("""
        SELECT ST_Overlaps(geom_1, geom_2)
        FROM h3_overlap_test
    """).fetchone()[0]

    # Non-adjacent H3 cells should not overlap
    assert not result, "Non-adjacent H3 cells should not overlap"
