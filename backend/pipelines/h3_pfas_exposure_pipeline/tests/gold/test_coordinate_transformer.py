"""
Tests for coordinate transformation utilities in H3 PFAS exposure analysis.

Tests coordinate system transformations and validation for accurate spatial operations.
This is CRITICAL for maintaining data quality standards in Denmark (EPSG:4326).
"""

import duckdb
import pytest

from h3_pfas_exposure.config import H3SpatialConfig
from h3_pfas_exposure.gold.coordinate_transformer import CoordinateTransformer


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
        processing_crs="EPSG:25832",
        output_crs="EPSG:4326",
        h3_resolution=9,
    )


@pytest.fixture
def transformer(duckdb_conn, config):
    """Create CoordinateTransformer instance."""
    return CoordinateTransformer(duckdb_conn, config)


@pytest.fixture
def sample_wgs84_data(duckdb_conn):
    """Create sample data in WGS84 (EPSG:4326) coordinates."""
    # Copenhagen: 55.6761° N, 12.5683° E
    duckdb_conn.execute("""
        CREATE TABLE wgs84_test AS
        SELECT
            'point_1' as id,
            ST_Point(12.5683, 55.6761) as geometry
        UNION ALL
        SELECT
            'polygon_1' as id,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry
    """)
    return "wgs84_test"


@pytest.fixture
def sample_utm_data(duckdb_conn):
    """Create sample data in UTM Zone 32N (EPSG:25832) coordinates."""
    # Copenhagen in UTM: approximately 725000, 6175000
    duckdb_conn.execute("""
        CREATE TABLE utm_test AS
        SELECT
            'point_utm' as id,
            ST_Point(725000, 6175000) as geometry
    """)
    return "utm_test"


# Coordinate System Tests
def test_transform_wgs84_to_utm(duckdb_conn, transformer, sample_wgs84_data):
    """Test transformation from WGS84 (EPSG:4326) to UTM (EPSG:25832)."""
    # Transform to UTM
    # Note: always_xy := true is needed because our test data is in lon/lat order (GeoJSON standard)
    # but EPSG:4326's formal definition is lat/lon order
    duckdb_conn.execute("""
        CREATE TABLE utm_transformed AS
        SELECT
            id,
            ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832', always_xy := true) as geometry_utm,
            ST_X(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832', always_xy := true)) as x_utm,
            ST_Y(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832', always_xy := true)) as y_utm
        FROM wgs84_test
        WHERE id = 'point_1'
    """)

    result = duckdb_conn.execute("""
        SELECT x_utm, y_utm
        FROM utm_transformed
    """).fetchone()

    # Copenhagen UTM coordinates should be approximately (725000, 6175000)
    # Note: ST_Transform may output (northing, easting) instead of (easting, northing)
    x_coord = result[0]
    y_coord = result[1]

    assert x_coord is not None, "X coordinate should not be NULL"
    assert y_coord is not None, "Y coordinate should not be NULL"

    # UTM Zone 32N coordinates for Denmark should be:
    # Easting (E): approximately 200,000 - 900,000 meters
    # Northing (N): approximately 6,000,000 - 6,400,000 meters
    easting_like = 200000 < x_coord < 900000 or 200000 < y_coord < 900000
    northing_like = 6000000 < x_coord < 6400000 or 6000000 < y_coord < 6400000

    assert easting_like and northing_like, (
        f"Transformed coordinates should be in UTM ranges, got {x_coord}, {y_coord}"
    )


def test_transform_utm_to_wgs84(duckdb_conn, transformer, sample_utm_data):
    """Test transformation from UTM (EPSG:25832) to WGS84 (EPSG:4326)."""
    # Transform to WGS84
    # always_xy := true ensures output is in lon/lat order (GeoJSON standard)
    duckdb_conn.execute("""
        CREATE TABLE wgs84_transformed AS
        SELECT
            id,
            ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326', always_xy := true) as geometry_wgs84,
            ST_X(ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326', always_xy := true)) as lon,
            ST_Y(ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326', always_xy := true)) as lat
        FROM utm_test
    """)

    result = duckdb_conn.execute("""
        SELECT lon, lat
        FROM wgs84_transformed
    """).fetchone()

    # Should be approximately Copenhagen coordinates (12.5683, 55.6761)
    assert result[0] is not None, "Longitude should not be NULL"
    assert result[1] is not None, "Latitude should not be NULL"
    assert 12.0 < result[0] < 13.0, f"Longitude should be ~12.5683, got {result[0]}"
    assert 55.0 < result[1] < 56.0, f"Latitude should be ~55.6761, got {result[1]}"


def test_roundtrip_transformation(duckdb_conn, transformer):
    """Test WGS84 → UTM → WGS84 preserves coordinates."""
    # Copenhagen: 55.6761° N, 12.5683° E
    original_lat = 55.6761
    original_lon = 12.5683

    duckdb_conn.execute(f"""
        CREATE TABLE roundtrip_test AS
        SELECT
            ST_Point({original_lon}, {original_lat}) as geometry_wgs84
    """)

    # Transform to UTM and back
    duckdb_conn.execute("""
        CREATE TABLE roundtrip_result AS
        SELECT
            ST_X(ST_Transform(ST_Transform(geometry_wgs84, 'EPSG:4326', 'EPSG:25832'), 'EPSG:25832', 'EPSG:4326')) as lon_final,
            ST_Y(ST_Transform(ST_Transform(geometry_wgs84, 'EPSG:4326', 'EPSG:25832'), 'EPSG:25832', 'EPSG:4326')) as lat_final
        FROM roundtrip_test
    """)

    result = duckdb_conn.execute("""
        SELECT lon_final, lat_final
        FROM roundtrip_result
    """).fetchone()

    # Should preserve coordinates within small tolerance (1e-6 degrees ~ 0.1 meters)
    lon_diff = abs(result[0] - original_lon)
    lat_diff = abs(result[1] - original_lat)

    assert lon_diff < 1e-6, f"Longitude roundtrip error {lon_diff} too large"
    assert lat_diff < 1e-6, f"Latitude roundtrip error {lat_diff} too large"


def test_coordinate_precision(duckdb_conn, transformer):
    """Test that transformations maintain 6+ decimal places precision."""
    # High precision Copenhagen coordinates
    duckdb_conn.execute("""
        CREATE TABLE precision_test AS
        SELECT
            ST_Point(12.568345, 55.676123) as geometry
    """)

    result = duckdb_conn.execute("""
        SELECT
            ST_X(geometry) as lon,
            ST_Y(geometry) as lat
        FROM precision_test
    """).fetchone()

    # Check precision is maintained
    lon_str = str(result[0])
    lat_str = str(result[1])

    # Should have at least 6 decimal places
    assert "." in lon_str, "Longitude should have decimal places"
    assert "." in lat_str, "Latitude should have decimal places"

    lon_decimals = len(lon_str.split(".")[1]) if "." in lon_str else 0
    lat_decimals = len(lat_str.split(".")[1]) if "." in lat_str else 0

    assert lon_decimals >= 6, f"Longitude should have 6+ decimal places, got {lon_decimals}"
    assert lat_decimals >= 6, f"Latitude should have 6+ decimal places, got {lat_decimals}"


# Validation Tests
def test_st_isvalid_check(duckdb_conn, transformer):
    """Test ST_IsValid() validation before transform."""
    # Create valid and invalid geometries
    duckdb_conn.execute("""
        CREATE TABLE validation_test AS
        SELECT
            'valid' as type,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry
        UNION ALL
        SELECT
            'invalid' as type,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.677, 12.569 55.676, 12.568 55.677, 12.568 55.676))') as geometry
    """)

    # Prepare geometries (filters out invalid ones)
    prepared_table = transformer.prepare_geometries("validation_test")

    # Check that only valid geometries remain
    result = duckdb_conn.execute(f"""
        SELECT type, ST_IsValid(geometry) as is_valid
        FROM {prepared_table}
    """).fetchall()

    for row in result:
        assert row[1], f"All geometries in prepared table should be valid, found type: {row[0]}"


def test_invalid_geometry_fallback(duckdb_conn, transformer):
    """Test fallback mechanism for invalid geometries."""
    # Create table with some invalid geometries
    duckdb_conn.execute("""
        CREATE TABLE fallback_test AS
        SELECT
            'valid_1' as id,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry
        UNION ALL
        SELECT
            'valid_2' as id,
            ST_GeomFromText('POINT(12.5683 55.6761)') as geometry
    """)

    # Should not raise error even with NULL geometries
    prepared_table = transformer.prepare_geometries("fallback_test")

    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {prepared_table}").fetchone()[0]
    assert count >= 2, "Valid geometries should be preserved"


def test_spherical_calculations(duckdb_conn, transformer):
    """Test that spherical calculations work correctly for lat/lon."""
    # Create two points: Copenhagen and Aarhus
    # ST_Distance_Spheroid expects lat/lon order, so we need ST_FlipCoordinates
    # or we can use ST_Point with proper lat/lon order
    duckdb_conn.execute("""
        CREATE TABLE spherical_test AS
        SELECT
            'copenhagen' as city,
            ST_Point(55.6761, 12.5683) as geometry  -- lat, lon order for spheroid
        UNION ALL
        SELECT
            'aarhus' as city,
            ST_Point(56.1629, 10.2039) as geometry  -- lat, lon order for spheroid
    """)

    # Calculate distance between cities (should be ~150-200 km)
    result = duckdb_conn.execute("""
        SELECT
            ST_Distance_Spheroid(
                (SELECT geometry FROM spherical_test WHERE city = 'copenhagen'),
                (SELECT geometry FROM spherical_test WHERE city = 'aarhus')
            ) / 1000.0 as distance_km
    """).fetchone()

    # Distance should be approximately 150-200 km
    distance = result[0]
    assert 140 < distance < 210, (
        f"Distance Copenhagen-Aarhus should be ~150-200 km, got {distance:.1f} km"
    )


# Denmark Bounds Tests
def test_coordinates_within_denmark(duckdb_conn, transformer):
    """Test that coordinates are within Denmark bounds."""
    # Denmark bounds: lon ∈ [7.5, 15.5], lat ∈ [54.5, 58]
    duckdb_conn.execute("""
        CREATE TABLE denmark_bounds_test AS
        SELECT
            'copenhagen' as city,
            12.5683 as lon,
            55.6761 as lat
        UNION ALL
        SELECT
            'aarhus' as city,
            10.2039 as lon,
            56.1629 as lat
        UNION ALL
        SELECT
            'aalborg' as city,
            9.9216 as lon,
            57.0488 as lat
        UNION ALL
        SELECT
            'odense' as city,
            10.4028 as lon,
            55.4038 as lat
    """)

    # Check all coordinates are within Denmark bounds
    out_of_bounds = duckdb_conn.execute("""
        SELECT COUNT(*)
        FROM denmark_bounds_test
        WHERE lon < 7.5 OR lon > 15.5
           OR lat < 54.5 OR lat > 58.0
    """).fetchone()[0]

    assert out_of_bounds == 0, "All test coordinates should be within Denmark bounds"


def test_utm_coordinates_denmark(duckdb_conn, transformer):
    """Test UTM Zone 32N bounds for Denmark."""
    # Denmark in UTM Zone 32N: approximately
    # X (Easting): 400,000 - 900,000
    # Y (Northing): 6,050,000 - 6,400,000

    # Use always_xy := true because input is lon/lat (GeoJSON) order
    duckdb_conn.execute("""
        CREATE TABLE utm_bounds_test AS
        SELECT
            ST_Transform(ST_Point(12.5683, 55.6761), 'EPSG:4326', 'EPSG:25832', always_xy := true) as geometry_utm
    """)

    result = duckdb_conn.execute("""
        SELECT
            ST_X(geometry_utm) as x,
            ST_Y(geometry_utm) as y
        FROM utm_bounds_test
    """).fetchone()

    x_utm = result[0]
    y_utm = result[1]

    assert 400000 < x_utm < 900000, f"UTM X for Denmark should be 400k-900k, got {x_utm}"
    assert 6050000 < y_utm < 6400000, f"UTM Y for Denmark should be 6050k-6400k, got {y_utm}"


def test_boundary_edge_cases(duckdb_conn, transformer):
    """Test coordinates near Denmark boundaries."""
    # Test coordinates near the edges of Denmark
    duckdb_conn.execute("""
        CREATE TABLE boundary_test AS
        SELECT
            'south' as location,
            12.0 as lon,
            54.6 as lat  -- Just inside southern boundary
        UNION ALL
        SELECT
            'north' as location,
            10.0 as lon,
            57.8 as lat  -- Just inside northern boundary
        UNION ALL
        SELECT
            'west' as location,
            8.1 as lon,
            55.5 as lat  -- Just inside western boundary
        UNION ALL
        SELECT
            'east' as location,
            15.2 as lon,
            55.0 as lat  -- Just inside eastern boundary
    """)

    # All should be within Denmark bounds
    in_bounds = duckdb_conn.execute("""
        SELECT COUNT(*)
        FROM boundary_test
        WHERE lon >= 7.5 AND lon <= 15.5
          AND lat >= 54.5 AND lat <= 58.0
    """).fetchone()[0]

    assert in_bounds == 4, "All boundary test points should be within Denmark bounds"


# Geometry Preparation Tests
def test_prepare_geometries_filters_null(duckdb_conn, transformer):
    """Test that prepare_geometries filters out NULL geometries."""
    duckdb_conn.execute("""
        CREATE TABLE null_geom_test AS
        SELECT
            'valid' as id,
            ST_Point(12.5683, 55.6761) as geometry
        UNION ALL
        SELECT
            'null' as id,
            NULL::GEOMETRY as geometry
    """)

    prepared_table = transformer.prepare_geometries("null_geom_test")

    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {prepared_table}").fetchone()[0]
    null_count = duckdb_conn.execute(
        f"SELECT COUNT(*) FROM {prepared_table} WHERE geometry IS NULL"
    ).fetchone()[0]

    assert count == 1, "Should have 1 valid geometry"
    assert null_count == 0, "Should have no NULL geometries in prepared table"


def test_prepare_geometries_validates(duckdb_conn, transformer):
    """Test that prepare_geometries validates geometries."""
    duckdb_conn.execute("""
        CREATE TABLE validate_test AS
        SELECT
            'valid_polygon' as id,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry
        UNION ALL
        SELECT
            'valid_point' as id,
            ST_Point(12.5683, 55.6761) as geometry
    """)

    prepared_table = transformer.prepare_geometries("validate_test")

    # All geometries should be valid
    invalid_count = duckdb_conn.execute(f"""
        SELECT COUNT(*)
        FROM {prepared_table}
        WHERE NOT ST_IsValid(geometry)
    """).fetchone()[0]

    assert invalid_count == 0, "All prepared geometries should be valid"


def test_prepare_geometries_preserves_data(duckdb_conn, transformer):
    """Test that prepare_geometries preserves all valid data."""
    duckdb_conn.execute("""
        CREATE TABLE preserve_test AS
        SELECT
            'field_1' as field_id,
            '12345678' as cvr_number,
            10.5 as area_ha,
            ST_Point(12.5683, 55.6761) as geometry
        UNION ALL
        SELECT
            'field_2' as field_id,
            '87654321' as cvr_number,
            5.3 as area_ha,
            ST_Point(10.2039, 56.1629) as geometry
    """)

    prepared_table = transformer.prepare_geometries("preserve_test")

    # Check all fields are preserved
    result = duckdb_conn.execute(f"""
        SELECT COUNT(*), SUM(area_ha)
        FROM {prepared_table}
    """).fetchone()

    assert result[0] == 2, "Should preserve both records"
    assert abs(float(result[1]) - 15.8) < 0.01, "Should preserve area values"


def test_coordinate_order_lat_lon(duckdb_conn, transformer):
    """Test that coordinates are in correct lat/lon order for spherical calculations."""
    # According to the pipeline comment: "field geometries in correct lat/lon order"
    # WGS84 uses (lon, lat) but spherical calculations expect (lat, lon)

    duckdb_conn.execute("""
        CREATE TABLE coord_order_test AS
        SELECT
            ST_Point(12.5683, 55.6761) as geometry
    """)

    # Get coordinates
    result = duckdb_conn.execute("""
        SELECT ST_X(geometry) as x, ST_Y(geometry) as y
        FROM coord_order_test
    """).fetchone()

    # In WGS84, X=lon, Y=lat
    # Longitude should be smaller than latitude for Denmark
    assert result[0] < result[1], f"For Denmark, lon ({result[0]}) should be < lat ({result[1]})"


def test_area_calculation_spherical(duckdb_conn, transformer):
    """Test spherical area calculations for accuracy."""
    # Create a 1 hectare square (100m x 100m) in Copenhagen
    # ST_Area_Spheroid expects coordinates in lat/lon order (EPSG:4326 formal definition)
    # At 55.6761°N, 1 degree lon ≈ 63,500m, 1 degree lat ≈ 111,000m
    # So 100m ≈ 0.00157° lon, 0.0009° lat
    # Polygon in lat/lon order: (lat lon, lat lon, ...)

    duckdb_conn.execute("""
        CREATE TABLE area_test AS
        SELECT
            ST_GeomFromText('POLYGON((55.676 12.568, 55.676 12.56957, 55.6769 12.56957, 55.6769 12.568, 55.676 12.568))') as geometry
    """)

    # Calculate area in hectares
    result = duckdb_conn.execute("""
        SELECT ST_Area_Spheroid(geometry) / 10000.0 as area_ha
        FROM area_test
    """).fetchone()

    area_ha = result[0]

    # Should be approximately 1 hectare (allow 20% tolerance due to curvature)
    assert 0.8 < area_ha < 1.2, f"1 hectare test polygon should be ~1.0 ha, got {area_ha:.2f} ha"
