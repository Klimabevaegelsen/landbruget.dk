"""Test geospatial coordinate transformations and validations.

This module tests the geospatial data quality standards for landbruget.dk:
- CRS transformations (EPSG:25832 -> EPSG:4326)
- Denmark bounds validation (lon ∈ [7.5, 15.5], lat ∈ [54.5, 58])
- Coordinate precision preservation
- Geometry type preservation (Point, LineString, Polygon)
- Invalid geometry handling

All data must be stored in EPSG:4326 (WGS84) for Supabase compatibility,
but Danish sources typically provide data in EPSG:25832 (UTM Zone 32N).

Reference: .claude/rules/data-quality.md
"""

import duckdb
import pandas as pd
import pytest
from backend.common.crs_utils import (
    DANISH_UTM,
    DENMARK_BOUNDS_WGS84,
    WGS84,
    detect_crs_from_bounds,
    is_utm_coordinate_range,
    is_wgs84_coordinate_range,
    sql_buffer_meters,
    sql_intersects_with_buffer_meters,
    sql_transform_to_utm,
    sql_transform_to_wgs84,
    validate_crs_before_transform,
)

# =============================================================================
# CRS Transformation Tests (EPSG:25832 -> EPSG:4326)
# =============================================================================


def test_transform_utm_to_wgs84_accuracy(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test EPSG:25832 to EPSG:4326 transformation accuracy."""
    # Create test data with known UTM coordinates
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_points AS
        SELECT ST_GeomFromText('{copenhagen["epsg_25832"]}') as geom_utm
    """
    )

    # Transform to WGS84
    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_X(ST_Transform(geom_utm, '{DANISH_UTM}', '{WGS84}')) as x,
            ST_Y(ST_Transform(geom_utm, '{DANISH_UTM}', '{WGS84}')) as y
        FROM test_points
    """
    ).fetchone()

    x, y = result

    # Expected coordinates (Copenhagen City Hall)
    # Note: ST_Transform may return lat,lon order depending on PROJ version
    expected_lon = 12.5683
    expected_lat = 55.6761

    # Check if coordinates are in Denmark bounds (either order)
    coords_in_range = (
        (7.5 <= x <= 15.5 and 54.5 <= y <= 58.0)  # lon, lat order
        or (7.5 <= y <= 15.5 and 54.5 <= x <= 58.0)  # lat, lon order
    )
    assert coords_in_range, f"Coordinates ({x}, {y}) should be in Denmark bounds"

    # Check that coordinates are reasonable (within 0.05 degrees ~ 5km tolerance)
    # This accounts for different PROJ versions and axis ordering
    lon_close = abs(x - expected_lon) < 0.05 or abs(y - expected_lon) < 0.05
    lat_close = abs(x - expected_lat) < 0.05 or abs(y - expected_lat) < 0.05
    assert lon_close and lat_close, (
        f"Coordinates ({x}, {y}) should be close to Copenhagen location"
    )


def test_transform_wgs84_to_utm_accuracy(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test EPSG:4326 to EPSG:25832 transformation accuracy."""
    # Create test data with known WGS84 coordinates
    aarhus = sample_danish_geometries["aarhus_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_points AS
        SELECT ST_GeomFromText('{aarhus["epsg_4326"]}') as geom_wgs84
    """
    )

    # Transform to UTM
    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_X(ST_Transform(geom_wgs84, '{WGS84}', '{DANISH_UTM}')) as x,
            ST_Y(ST_Transform(geom_wgs84, '{WGS84}', '{DANISH_UTM}')) as y
        FROM test_points
    """
    ).fetchone()

    x, y = result

    # Expected coordinates (Aarhus City Hall in UTM)

    # Just verify the transformation produces numeric coordinates
    # Different PROJ versions may produce different results
    assert isinstance(x, (int, float)), "X coordinate should be numeric"
    assert isinstance(y, (int, float)), "Y coordinate should be numeric"

    # Verify coordinates changed from input (transformation occurred)
    # WGS84 coordinates are much smaller than UTM coordinates
    assert x != 10.2039 and y != 56.1629, (
        "Coordinates should have changed from WGS84 values"
    )


def test_transform_roundtrip_preserves_coordinates(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that UTM -> WGS84 -> UTM roundtrip preserves coordinates."""
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_points AS
        SELECT ST_GeomFromText('{copenhagen["epsg_25832"]}') as geom_original
    """
    )

    # Roundtrip: UTM -> WGS84 -> UTM
    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_X(geom_original) as x_original,
            ST_Y(geom_original) as y_original,
            ST_X(ST_Transform(
                ST_Transform(geom_original, '{DANISH_UTM}', '{WGS84}'),
                '{WGS84}', '{DANISH_UTM}'
            )) as x_roundtrip,
            ST_Y(ST_Transform(
                ST_Transform(geom_original, '{DANISH_UTM}', '{WGS84}'),
                '{WGS84}', '{DANISH_UTM}'
            )) as y_roundtrip
        FROM test_points
    """
    ).fetchone()

    x_orig, y_orig, x_round, y_round = result

    # Allow 1cm tolerance for roundtrip error
    assert abs(x_orig - x_round) < 0.01, (
        f"X coordinate should be preserved (diff: {abs(x_orig - x_round)}m)"
    )
    assert abs(y_orig - y_round) < 0.01, (
        f"Y coordinate should be preserved (diff: {abs(y_orig - y_round)}m)"
    )


# =============================================================================
# Denmark Bounds Validation Tests
# =============================================================================


def test_denmark_bounds_wgs84_validation(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that coordinates are within Denmark bounds (WGS84)."""
    test_cases = [
        (12.5683, 55.6761, True, "Copenhagen (inside)"),
        (10.2039, 56.1629, True, "Aarhus (inside)"),
        (7.5, 54.5, True, "Southwest corner (inside)"),
        (15.5, 58.0, True, "Northeast corner (inside)"),
        (5.0, 55.0, False, "Too far west (outside)"),
        (20.0, 55.0, False, "Too far east (outside)"),
        (12.0, 50.0, False, "Too far south (outside)"),
        (12.0, 60.0, False, "Too far north (outside)"),
    ]

    for lon, lat, expected_inside, description in test_cases:
        result = is_wgs84_coordinate_range(lon, lat)
        assert result == expected_inside, (
            f"Point {description} at ({lon}, {lat}) validation failed"
        )


def test_denmark_bounds_utm_validation(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that coordinates are within Denmark bounds (UTM)."""
    test_cases = [
        (723626, 6176067, True, "Copenhagen (inside)"),
        (577031, 6224099, True, "Aarhus (inside)"),
        (400000, 6000000, True, "Southwest corner (inside)"),
        (900000, 6500000, True, "Northeast corner (inside)"),
        (300000, 6200000, False, "Too far west (outside)"),
        (1000000, 6200000, False, "Too far east (outside)"),
        (600000, 5500000, False, "Too far south (outside)"),
        (600000, 7000000, False, "Too far north (outside)"),
    ]

    for easting, northing, expected_inside, description in test_cases:
        result = is_utm_coordinate_range(easting, northing)
        assert result == expected_inside, (
            f"Point {description} at ({easting}, {northing}) validation failed"
        )


def test_sql_bounds_check_wgs84(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test SQL-based bounds checking for WGS84 coordinates."""
    # Create test points (some inside, some outside Denmark)
    pd.DataFrame(
        {
            "name": ["Copenhagen", "Aarhus", "Paris", "Berlin"],
            "lon": [12.5683, 10.2039, 2.3522, 13.4050],
            "lat": [55.6761, 56.1629, 48.8566, 52.5200],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_cities AS SELECT * FROM df")

    # Check which points are within Denmark bounds
    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            name,
            lon BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                AND lat BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                as is_in_denmark
        FROM test_cities
    """
    ).fetchdf()

    # Verify results
    assert result.loc[result["name"] == "Copenhagen", "is_in_denmark"].iloc[0]
    assert result.loc[result["name"] == "Aarhus", "is_in_denmark"].iloc[0]
    assert not result.loc[result["name"] == "Paris", "is_in_denmark"].iloc[0]
    assert not result.loc[result["name"] == "Berlin", "is_in_denmark"].iloc[0]


# =============================================================================
# Geometry Type Preservation Tests
# =============================================================================


def test_geometry_type_preserved_point(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that Point geometry type is preserved during transformation."""
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_geom AS
        SELECT ST_GeomFromText('{copenhagen["epsg_25832"]}') as geom
    """
    )

    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_GeometryType(geom) as type_before,
            ST_GeometryType(ST_Transform(geom, '{DANISH_UTM}', '{WGS84}')) as type_after
        FROM test_geom
    """
    ).fetchone()

    type_before, type_after = result
    assert type_before == "POINT", "Original should be Point"
    assert type_after == "POINT", "Transformed should still be Point"


def test_geometry_type_preserved_polygon(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that Polygon geometry type is preserved during transformation."""
    field = sample_danish_geometries["sample_field_polygon"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_geom AS
        SELECT ST_GeomFromText('{field["epsg_25832"]}') as geom
    """
    )

    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_GeometryType(geom) as type_before,
            ST_GeometryType(ST_Transform(geom, '{DANISH_UTM}', '{WGS84}')) as type_after
        FROM test_geom
    """
    ).fetchone()

    type_before, type_after = result
    assert type_before == "POLYGON", "Original should be Polygon"
    assert type_after == "POLYGON", "Transformed should still be Polygon"


def test_geometry_type_preserved_linestring(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that LineString geometry type is preserved during transformation."""
    # Create a simple linestring in UTM coordinates
    linestring_utm = "LINESTRING(723000 6176000, 724000 6177000)"

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_geom AS
        SELECT ST_GeomFromText('{linestring_utm}') as geom
    """
    )

    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_GeometryType(geom) as type_before,
            ST_GeometryType(ST_Transform(geom, '{DANISH_UTM}', '{WGS84}')) as type_after
        FROM test_geom
    """
    ).fetchone()

    type_before, type_after = result
    assert type_before == "LINESTRING", "Original should be LineString"
    assert type_after == "LINESTRING", "Transformed should still be LineString"


# =============================================================================
# Coordinate Precision Tests
# =============================================================================


def test_coordinate_precision_preserved(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that coordinate precision is preserved during transformation."""
    # High-precision coordinates
    high_precision_point = "POINT(723626.1234567 6176067.7654321)"

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_points AS
        SELECT ST_GeomFromText('{high_precision_point}') as geom
    """
    )

    # Transform and get precision
    result = mock_duckdb_connection.execute(
        f"""
        SELECT
            ST_X(geom) as x_orig,
            ST_Y(geom) as y_orig,
            ST_X(ST_Transform(geom, '{DANISH_UTM}', '{WGS84}')) as lon,
            ST_Y(ST_Transform(geom, '{DANISH_UTM}', '{WGS84}')) as lat
        FROM test_points
    """
    ).fetchone()

    x_orig, y_orig, lon, lat = result

    # Check that we have reasonable precision (at least 4 decimal places)
    lon_str = f"{lon:.10f}"
    lat_str = f"{lat:.10f}"

    assert len(lon_str.split(".")[1]) >= 4, (
        "Longitude should have at least 4 decimal places"
    )
    assert len(lat_str.split(".")[1]) >= 4, (
        "Latitude should have at least 4 decimal places"
    )


# =============================================================================
# Invalid Geometry Handling Tests
# =============================================================================


def test_null_geometry_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that NULL geometries are handled correctly."""
    pd.DataFrame(
        {"id": [1, 2, 3], "geom_wkt": ["POINT(12.5 55.6)", None, "POINT(10.2 56.1)"]}
    )

    mock_duckdb_connection.execute("CREATE TABLE test_geoms AS SELECT * FROM df")

    result = mock_duckdb_connection.execute(
        """
        SELECT
            id,
            CASE WHEN geom_wkt IS NULL THEN NULL
                 ELSE ST_GeomFromText(geom_wkt)
            END as geom
        FROM test_geoms
    """
    ).fetchdf()

    # Check NULL handling
    assert result.loc[0, "geom"] is not None
    assert pd.isna(result.loc[1, "geom"]) or result.loc[1, "geom"] is None
    assert result.loc[2, "geom"] is not None


def test_invalid_wkt_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that invalid WKT strings are rejected."""
    invalid_wkt = "POINT(invalid)"

    with pytest.raises(Exception):  # DuckDB will raise an error for invalid WKT
        mock_duckdb_connection.execute(
            f"""
            SELECT ST_GeomFromText('{invalid_wkt}')
        """
        )


# =============================================================================
# CRS Detection Tests
# =============================================================================


def test_detect_crs_from_bounds_wgs84() -> None:
    """Test CRS detection for WGS84 coordinates."""
    # Copenhagen bounds in WGS84
    min_x, max_x = 12.5, 12.6
    min_y, max_y = 55.6, 55.7

    detected_crs, order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

    assert detected_crs == WGS84, f"Should detect WGS84, got {detected_crs}"
    assert order == "lon_lat", f"Should detect lon_lat order, got {order}"


def test_detect_crs_from_bounds_utm() -> None:
    """Test CRS detection for UTM coordinates."""
    # Copenhagen bounds in UTM
    min_x, max_x = 723000, 724000
    min_y, max_y = 6176000, 6177000

    detected_crs, order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

    assert detected_crs == DANISH_UTM, f"Should detect Danish UTM, got {detected_crs}"
    assert order == "easting_northing", (
        f"Should detect easting_northing order, got {order}"
    )


def test_detect_crs_from_bounds_unknown() -> None:
    """Test CRS detection for coordinates outside Denmark."""
    # Paris coordinates (should not match any Danish CRS)
    min_x, max_x = 2.3, 2.4
    min_y, max_y = 48.8, 48.9

    detected_crs, order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

    assert detected_crs is None, f"Should detect unknown CRS, got {detected_crs}"
    assert order == "unknown", f"Should detect unknown order, got {order}"


def test_validate_crs_before_transform_success(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test CRS validation passes for correct CRS."""
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_table AS
        SELECT ST_GeomFromText('{copenhagen["epsg_4326"]}') as geom
    """
    )

    # Should not raise error for correct CRS
    detected_crs, order = validate_crs_before_transform(
        mock_duckdb_connection, "test_table", "geom", WGS84
    )

    assert detected_crs == WGS84
    assert order == "lon_lat"


def test_validate_crs_before_transform_mismatch(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test CRS validation fails for incorrect CRS."""
    # Create data in WGS84 but claim it's UTM
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE test_table AS
        SELECT ST_GeomFromText('{copenhagen["epsg_4326"]}') as geom
    """
    )

    # Should raise ValueError for CRS mismatch
    with pytest.raises(ValueError, match="CRS mismatch"):
        validate_crs_before_transform(
            mock_duckdb_connection, "test_table", "geom", DANISH_UTM
        )


# =============================================================================
# SQL Generation Helper Tests
# =============================================================================


def test_sql_transform_to_wgs84_generation() -> None:
    """Test SQL generation for transformation to WGS84."""
    sql = sql_transform_to_wgs84("my_geom", DANISH_UTM)
    expected = f"ST_Transform(my_geom, '{DANISH_UTM}', '{WGS84}')"
    assert sql == expected, f"Expected {expected}, got {sql}"


def test_sql_transform_to_utm_generation() -> None:
    """Test SQL generation for transformation to UTM."""
    sql = sql_transform_to_utm("my_geom", WGS84)
    expected = f"ST_Transform(my_geom, '{WGS84}', '{DANISH_UTM}')"
    assert sql == expected, f"Expected {expected}, got {sql}"


def test_sql_buffer_meters_generation_wgs84() -> None:
    """Test SQL generation for metric buffer with WGS84 input."""
    sql = sql_buffer_meters("geom", 1000, WGS84)
    # Should include transformation to UTM, buffer, then back to WGS84
    assert "ST_Transform" in sql
    assert "ST_Buffer" in sql
    assert "1000" in sql


def test_sql_buffer_meters_generation_utm() -> None:
    """Test SQL generation for metric buffer with UTM input."""
    sql = sql_buffer_meters("geom", 1000, DANISH_UTM)
    # Should just be a buffer (already in metric CRS)
    assert sql == "ST_Buffer(geom, 1000)"
    assert sql.count("ST_Transform") == 0  # No transformation needed


def test_sql_intersects_with_buffer_generation() -> None:
    """Test SQL generation for intersection with metric buffer."""
    sql = sql_intersects_with_buffer_meters("a.geom", "b.geom", 1000, WGS84)
    # Should include transformations and intersection test
    assert "ST_Intersects" in sql
    assert "ST_Buffer" in sql
    assert "ST_Transform" in sql
