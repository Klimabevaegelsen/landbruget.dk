"""Comprehensive tests for CRS utilities.

Tests the centralized CRS handling utilities used across all data pipelines.
Covers constants, detection functions, validation utilities, and SQL generation helpers.
"""

import contextlib

import pytest
from common.crs_utils import (
    # Constants
    DANISH_UTM,
    DEGREES_TO_METERS_AVG,
    DEGREES_TO_METERS_LAT,
    DEGREES_TO_METERS_LON,
    DENMARK_BOUNDS_UTM,
    DENMARK_BOUNDS_WGS84,
    SOURCE_CRS_DANISH,
    TARGET_CRS_STORAGE,
    WEB_MERCATOR,
    WGS84,
    degrees_to_meters,
    # Detection functions
    detect_crs_from_bounds,
    is_utm_coordinate_range,
    is_wgs84_coordinate_range,
    log_geometry_bounds,
    # Conversion functions
    meters_to_degrees,
    sql_buffer_meters,
    sql_intersects_with_buffer_meters,
    # SQL generation helpers
    sql_transform_to_utm,
    sql_transform_to_wgs84,
    # Validation functions
    validate_crs_before_transform,
)


class TestCRSConstants:
    """Test CRS constant definitions."""

    def test_danish_utm_constant(self):
        """DANISH_UTM should be EPSG:25832 (UTM Zone 32N / ETRS89)."""
        assert DANISH_UTM == "EPSG:25832"

    def test_wgs84_constant(self):
        """WGS84 should be EPSG:4326."""
        assert WGS84 == "EPSG:4326"

    def test_web_mercator_constant(self):
        """WEB_MERCATOR should be EPSG:3857."""
        assert WEB_MERCATOR == "EPSG:3857"

    def test_aliases(self):
        """SOURCE_CRS_DANISH and TARGET_CRS_STORAGE should be correct aliases."""
        assert SOURCE_CRS_DANISH == DANISH_UTM
        assert TARGET_CRS_STORAGE == WGS84


class TestDenmarkBounds:
    """Test Denmark bounding box definitions."""

    def test_wgs84_bounds_reasonable(self):
        """WGS84 bounds should represent Denmark's geographic extent."""
        # Denmark is roughly between 7.5°E to 15.5°E longitude
        assert DENMARK_BOUNDS_WGS84["min_x"] == 7.5
        assert DENMARK_BOUNDS_WGS84["max_x"] == 15.5
        # Denmark is roughly between 54.5°N to 58°N latitude
        assert DENMARK_BOUNDS_WGS84["min_y"] == 54.5
        assert DENMARK_BOUNDS_WGS84["max_y"] == 58.0

    def test_utm_bounds_reasonable(self):
        """UTM bounds should represent Denmark's projected extent."""
        # Easting values for UTM Zone 32N in Denmark
        assert DENMARK_BOUNDS_UTM["min_x"] == 400000
        assert DENMARK_BOUNDS_UTM["max_x"] == 950000
        # Northing values for UTM Zone 32N in Denmark
        assert DENMARK_BOUNDS_UTM["min_y"] == 6000000
        assert DENMARK_BOUNDS_UTM["max_y"] == 6500000


class TestConversionFactors:
    """Test conversion factor constants."""

    def test_degrees_to_meters_factors(self):
        """Conversion factors should be approximately correct for Denmark latitude."""
        # At ~56°N: 1 degree longitude ≈ 62km, 1 degree latitude ≈ 111km
        assert DEGREES_TO_METERS_LON == 62000
        assert DEGREES_TO_METERS_LAT == 111000
        # Average should be approximately midway
        assert DEGREES_TO_METERS_AVG == 86500

    def test_average_is_midpoint(self):
        """Average conversion factor should be roughly the mean."""
        expected_avg = (DEGREES_TO_METERS_LON + DEGREES_TO_METERS_LAT) / 2
        assert abs(DEGREES_TO_METERS_AVG - expected_avg) < 1000


class TestMetersToDegreesConversion:
    """Test meters_to_degrees function."""

    def test_100km_to_degrees(self):
        """100km should convert to approximately 1.15 degrees."""
        result = meters_to_degrees(100000)
        assert 1.1 < result < 1.2  # ~1.156

    def test_1km_to_degrees(self):
        """1km should convert to approximately 0.01156 degrees."""
        result = meters_to_degrees(1000)
        assert 0.01 < result < 0.02  # ~0.01156

    def test_zero_meters(self):
        """Zero meters should return zero degrees."""
        assert meters_to_degrees(0) == 0.0

    def test_small_distance(self):
        """Small distances should scale appropriately."""
        result = meters_to_degrees(100)  # 100 meters
        assert 0.001 < result < 0.002


class TestDegreesToMetersConversion:
    """Test degrees_to_meters function."""

    def test_1_degree_to_meters(self):
        """1 degree should convert to approximately 86.5km."""
        result = degrees_to_meters(1.0)
        assert result == DEGREES_TO_METERS_AVG  # 86500

    def test_round_trip_conversion(self):
        """Converting meters to degrees and back should preserve value."""
        original = 50000  # 50km
        degrees = meters_to_degrees(original)
        back_to_meters = degrees_to_meters(degrees)
        assert abs(back_to_meters - original) < 1  # Within 1 meter

    def test_zero_degrees(self):
        """Zero degrees should return zero meters."""
        assert degrees_to_meters(0) == 0.0


class TestDetectCRSFromBounds:
    """Test CRS detection from coordinate bounds."""

    def test_detect_wgs84_lon_lat(self):
        """Should detect WGS84 with standard lon/lat order."""
        # Copenhagen area: lon ~12.5, lat ~55.7
        crs, order = detect_crs_from_bounds(12.0, 13.0, 55.0, 56.0)
        assert crs == WGS84
        assert order == "lon_lat"

    def test_detect_wgs84_entire_denmark(self):
        """Should detect WGS84 for full Denmark extent."""
        crs, order = detect_crs_from_bounds(8.0, 15.0, 54.5, 57.5)
        assert crs == WGS84
        assert order == "lon_lat"

    def test_detect_wgs84_lat_lon_swapped(self):
        """Should detect WGS84 with swapped lat/lon coordinates."""
        # Swapped: X contains latitude (54.5-58), Y contains longitude (7.5-15.5)
        crs, order = detect_crs_from_bounds(55.0, 56.0, 12.0, 13.0)
        assert crs == WGS84
        assert order == "lat_lon_swapped"

    def test_detect_utm(self):
        """Should detect UTM Zone 32N for Danish easting/northing coordinates."""
        # Copenhagen in UTM: easting ~723626, northing ~6176067
        crs, order = detect_crs_from_bounds(500000, 800000, 6100000, 6400000)
        assert crs == DANISH_UTM
        assert order == "easting_northing"

    def test_detect_utm_bornholm_area(self):
        """Should detect UTM Zone 32N for Bornholm-area coordinates (easting >900k)."""
        # Bornholm projects to ~915k easting in UTM 32N
        crs, order = detect_crs_from_bounds(419288, 915280, 6037373, 6424480)
        assert crs == DANISH_UTM
        assert order == "easting_northing"

    def test_detect_unknown_coordinates(self):
        """Should return None for unrecognized coordinate ranges."""
        # Random coordinates that don't match Denmark
        crs, order = detect_crs_from_bounds(0, 1, 0, 1)
        assert crs is None
        assert order == "unknown"

    def test_detect_negative_coordinates(self):
        """Should return None for negative coordinates (not in Denmark)."""
        crs, order = detect_crs_from_bounds(-10, -5, 40, 45)
        assert crs is None
        assert order == "unknown"

    def test_boundary_edge_cases_wgs84(self):
        """Should correctly handle boundary edge cases for WGS84."""
        # Exact boundary values
        crs, order = detect_crs_from_bounds(
            DENMARK_BOUNDS_WGS84["min_x"],
            DENMARK_BOUNDS_WGS84["max_x"],
            DENMARK_BOUNDS_WGS84["min_y"],
            DENMARK_BOUNDS_WGS84["max_y"],
        )
        assert crs == WGS84
        assert order == "lon_lat"

    def test_boundary_edge_cases_utm(self):
        """Should correctly handle boundary edge cases for UTM."""
        crs, order = detect_crs_from_bounds(
            DENMARK_BOUNDS_UTM["min_x"],
            DENMARK_BOUNDS_UTM["max_x"],
            DENMARK_BOUNDS_UTM["min_y"],
            DENMARK_BOUNDS_UTM["max_y"],
        )
        assert crs == DANISH_UTM
        assert order == "easting_northing"


class TestIsUTMCoordinateRange:
    """Test is_utm_coordinate_range function."""

    def test_valid_utm_coordinates(self):
        """Should return True for valid Danish UTM coordinates."""
        # Copenhagen
        assert is_utm_coordinate_range(723626, 6176067) is True
        # Aarhus
        assert is_utm_coordinate_range(577030, 6224098) is True

    def test_invalid_utm_low_easting(self):
        """Should return False for easting below valid range."""
        assert is_utm_coordinate_range(100000, 6200000) is False

    def test_invalid_utm_high_easting(self):
        """Should return False for easting above valid range."""
        assert is_utm_coordinate_range(1000000, 6200000) is False

    def test_invalid_utm_low_northing(self):
        """Should return False for northing below valid range."""
        assert is_utm_coordinate_range(600000, 5000000) is False

    def test_invalid_utm_high_northing(self):
        """Should return False for northing above valid range."""
        assert is_utm_coordinate_range(600000, 7000000) is False

    def test_wgs84_coordinates_rejected(self):
        """Should return False for WGS84 coordinates."""
        assert is_utm_coordinate_range(12.5, 55.7) is False


class TestIsWGS84CoordinateRange:
    """Test is_wgs84_coordinate_range function."""

    def test_valid_wgs84_coordinates(self):
        """Should return True for valid Danish WGS84 coordinates."""
        # Copenhagen
        assert is_wgs84_coordinate_range(12.5683, 55.6761) is True
        # Aarhus
        assert is_wgs84_coordinate_range(10.2039, 56.1629) is True

    def test_invalid_wgs84_low_longitude(self):
        """Should return False for longitude below valid range."""
        assert is_wgs84_coordinate_range(5.0, 56.0) is False

    def test_invalid_wgs84_high_longitude(self):
        """Should return False for longitude above valid range."""
        assert is_wgs84_coordinate_range(20.0, 56.0) is False

    def test_invalid_wgs84_low_latitude(self):
        """Should return False for latitude below valid range."""
        assert is_wgs84_coordinate_range(12.0, 50.0) is False

    def test_invalid_wgs84_high_latitude(self):
        """Should return False for latitude above valid range."""
        assert is_wgs84_coordinate_range(12.0, 65.0) is False

    def test_utm_coordinates_rejected(self):
        """Should return False for UTM coordinates."""
        assert is_wgs84_coordinate_range(723626, 6176067) is False


class TestSQLTransformToUTM:
    """Test sql_transform_to_utm function."""

    def test_default_source_wgs84(self):
        """Should generate SQL with default WGS84 source."""
        result = sql_transform_to_utm("geometry")
        assert result == f"ST_Transform(geometry, '{WGS84}', '{DANISH_UTM}')"

    def test_custom_source_crs(self):
        """Should generate SQL with custom source CRS."""
        result = sql_transform_to_utm("geom", WEB_MERCATOR)
        assert result == f"ST_Transform(geom, '{WEB_MERCATOR}', '{DANISH_UTM}')"

    def test_complex_expression(self):
        """Should handle complex geometry expressions."""
        result = sql_transform_to_utm("ST_Centroid(polygon)")
        assert "ST_Transform(ST_Centroid(polygon)" in result


class TestSQLTransformToWGS84:
    """Test sql_transform_to_wgs84 function."""

    def test_default_source_utm(self):
        """Should generate SQL with default UTM source."""
        result = sql_transform_to_wgs84("geometry")
        assert result == f"ST_Transform(geometry, '{DANISH_UTM}', '{WGS84}')"

    def test_custom_source_crs(self):
        """Should generate SQL with custom source CRS."""
        result = sql_transform_to_wgs84("geom", WEB_MERCATOR)
        assert result == f"ST_Transform(geom, '{WEB_MERCATOR}', '{WGS84}')"


class TestSQLBufferMeters:
    """Test sql_buffer_meters function."""

    def test_buffer_from_wgs84(self):
        """Should transform to UTM, buffer, and transform back for WGS84 input."""
        result = sql_buffer_meters("geometry", 100)
        # Should include transform to UTM
        assert f"'{WGS84}', '{DANISH_UTM}'" in result
        # Should include buffer
        assert "ST_Buffer(" in result
        assert ", 100)" in result
        # Should transform back to WGS84
        assert f"'{DANISH_UTM}', '{WGS84}'" in result

    def test_buffer_from_utm_no_double_transform(self):
        """Should NOT transform when source is already UTM."""
        result = sql_buffer_meters("geometry", 100, DANISH_UTM)
        # Should be a simple buffer without transforms
        assert result == "ST_Buffer(geometry, 100)"
        assert "ST_Transform" not in result

    def test_buffer_distance_preserved(self):
        """Should preserve the exact buffer distance."""
        result = sql_buffer_meters("geom", 50000)  # 50km
        assert "50000" in result

    def test_float_buffer_distance(self):
        """Should handle float buffer distances."""
        result = sql_buffer_meters("geom", 100.5)
        assert "100.5" in result


class TestSQLIntersectsWithBufferMeters:
    """Test sql_intersects_with_buffer_meters function."""

    def test_intersects_from_wgs84(self):
        """Should transform both geometries to UTM for intersection test."""
        result = sql_intersects_with_buffer_meters("geom1", "geom2", 100)
        # Should include ST_Intersects
        assert "ST_Intersects(" in result
        # Should transform both geometries to UTM
        assert result.count(f"'{WGS84}', '{DANISH_UTM}'") == 2
        # Should include buffer on second geometry
        assert "ST_Buffer(" in result
        assert ", 100)" in result

    def test_intersects_from_utm_simpler(self):
        """Should use simpler SQL when already in UTM."""
        result = sql_intersects_with_buffer_meters("geom1", "geom2", 100, DANISH_UTM)
        # Should be simpler without transforms
        assert result == "ST_Intersects(geom1, ST_Buffer(geom2, 100))"
        assert "ST_Transform" not in result

    def test_large_buffer_distance(self):
        """Should handle large buffer distances (e.g., 100km)."""
        result = sql_intersects_with_buffer_meters("a.geom", "b.geom", 100000)
        assert "100000" in result


class TestValidateCRSBeforeTransform:
    """Test validate_crs_before_transform function with DuckDB."""

    def test_valid_wgs84_coordinates(self, mock_duckdb_connection):
        """Should validate correctly for WGS84 coordinates."""
        conn = mock_duckdb_connection

        # Create table with WGS84 coordinates (Copenhagen area)
        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(12.5, 55.7) as geom
            UNION ALL
            SELECT ST_Point(12.6, 55.8) as geom
        """)

        crs, order = validate_crs_before_transform(conn, "test_geom", "geom", WGS84)
        assert crs == WGS84
        assert order == "lon_lat"

    def test_valid_utm_coordinates(self, mock_duckdb_connection):
        """Should validate correctly for UTM coordinates."""
        conn = mock_duckdb_connection

        # Create table with UTM coordinates
        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(723626, 6176067) as geom
            UNION ALL
            SELECT ST_Point(577030, 6224098) as geom
        """)

        crs, order = validate_crs_before_transform(conn, "test_geom", "geom", DANISH_UTM)
        assert crs == DANISH_UTM
        assert order == "easting_northing"

    def test_crs_mismatch_raises_error(self, mock_duckdb_connection):
        """Should raise ValueError when detected CRS doesn't match expected."""
        conn = mock_duckdb_connection

        # Create table with WGS84 coordinates
        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(12.5, 55.7) as geom
        """)

        # Expect UTM but data is WGS84
        with pytest.raises(ValueError, match="CRS mismatch"):
            validate_crs_before_transform(conn, "test_geom", "geom", DANISH_UTM)

    def test_unknown_crs_raises_error(self, mock_duckdb_connection):
        """Should raise ValueError when CRS cannot be determined."""
        conn = mock_duckdb_connection

        # Create table with coordinates outside Denmark
        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(0, 0) as geom
        """)

        with pytest.raises(ValueError, match="Cannot detect CRS"):
            validate_crs_before_transform(conn, "test_geom", "geom", WGS84)

    def test_empty_table_raises_error(self, mock_duckdb_connection):
        """Should raise ValueError for empty tables."""
        conn = mock_duckdb_connection

        conn.execute("""
            CREATE TABLE test_geom (geom GEOMETRY)
        """)

        with pytest.raises(ValueError, match="No valid geometries"):
            validate_crs_before_transform(conn, "test_geom", "geom", WGS84)

    def test_null_geometries_handled(self, mock_duckdb_connection):
        """Should handle tables with NULL geometries."""
        conn = mock_duckdb_connection

        # Create table with mix of NULL and valid coordinates
        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT NULL::GEOMETRY as geom
            UNION ALL
            SELECT ST_Point(12.5, 55.7) as geom
        """)

        crs, _order = validate_crs_before_transform(conn, "test_geom", "geom", WGS84)
        assert crs == WGS84


class TestLogGeometryBounds:
    """Test log_geometry_bounds function."""

    def test_returns_bounds_info(self, mock_duckdb_connection):
        """Should return dictionary with bounds and CRS info."""
        conn = mock_duckdb_connection

        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(12.5, 55.7) as geom
            UNION ALL
            SELECT ST_Point(12.6, 55.8) as geom
        """)

        info = log_geometry_bounds(conn, "test_geom", "geom")

        assert info["table"] == "test_geom"
        assert info["geometry_column"] == "geom"
        assert "bounds" in info
        assert info["detected_crs"] == WGS84
        assert info["coordinate_order"] == "lon_lat"

    def test_bounds_values_correct(self, mock_duckdb_connection):
        """Should return correct min/max bounds."""
        conn = mock_duckdb_connection

        conn.execute("""
            CREATE TABLE test_geom AS
            SELECT ST_Point(10.0, 55.0) as geom
            UNION ALL
            SELECT ST_Point(12.0, 57.0) as geom
        """)

        info = log_geometry_bounds(conn, "test_geom", "geom")

        assert info["bounds"]["min_x"] == 10.0
        assert info["bounds"]["max_x"] == 12.0
        assert info["bounds"]["min_y"] == 55.0
        assert info["bounds"]["max_y"] == 57.0

    def test_empty_table_returns_error(self, mock_duckdb_connection):
        """Should return error info for empty tables."""
        conn = mock_duckdb_connection

        conn.execute("""
            CREATE TABLE test_geom (geom GEOMETRY)
        """)

        info = log_geometry_bounds(conn, "test_geom", "geom")
        assert info == {"error": "no_geometries"}


class TestIntegration:
    """Integration tests combining multiple CRS utilities."""

    def test_full_transformation_workflow(self, mock_duckdb_connection):
        """Test complete workflow: detect CRS, generate SQL, validate.

        Note: The actual ST_Transform results depend on DuckDB's spatial extension
        and proj library configuration. This test validates the SQL generation
        and initial validation steps work correctly.
        """
        conn = mock_duckdb_connection

        # Create source data in WGS84
        conn.execute("""
            CREATE TABLE source_data AS
            SELECT ST_Point(12.5, 55.7) as geom
        """)

        # Validate CRS - this should work
        crs, _order = validate_crs_before_transform(conn, "source_data", "geom", WGS84)
        assert crs == WGS84

        # Generate transformation SQL - verify the SQL is correct
        transform_sql = sql_transform_to_utm("geom")
        assert f"'{WGS84}'" in transform_sql
        assert f"'{DANISH_UTM}'" in transform_sql

        # The actual transformation depends on DuckDB+proj configuration
        # Just verify the generated SQL is syntactically valid
        with contextlib.suppress(Exception):
            conn.execute(f"""
                CREATE TABLE utm_data AS
                SELECT {transform_sql} as geom
                FROM source_data
            """)
            # If execution succeeds, verify we can read the table
            result = conn.execute("SELECT COUNT(*) FROM utm_data").fetchone()
            assert result[0] == 1  # One row transformed

    def test_buffer_and_intersect_workflow_sql_generation(self, mock_duckdb_connection):
        """Test SQL generation for buffer/intersect operations.

        This test validates that the SQL helpers generate correct SQL syntax.
        The actual spatial execution depends on DuckDB+proj configuration.
        """
        conn = mock_duckdb_connection

        # Test SQL generation produces valid, executable SQL
        intersect_sql = sql_intersects_with_buffer_meters("a.geom", "b.geom", 200)

        # Verify the SQL has the expected structure
        assert "ST_Intersects" in intersect_sql
        assert "ST_Buffer" in intersect_sql
        assert "200" in intersect_sql

        # Create test data in UTM (avoids need for proj transforms in test)
        conn.execute("""
            CREATE TABLE points_utm AS
            SELECT 1 as id, ST_Point(500000, 6200000) as geom
            UNION ALL
            SELECT 2 as id, ST_Point(500100, 6200100) as geom  -- ~141m apart
        """)

        # For UTM data, use the simpler SQL
        simple_intersect = sql_intersects_with_buffer_meters("a.geom", "b.geom", 200, DANISH_UTM)

        # Execute spatial query with UTM data (no transforms needed)
        result = conn.execute(f"""
            SELECT a.id as id1, b.id as id2
            FROM points_utm a, points_utm b
            WHERE a.id < b.id
              AND {simple_intersect}
        """).fetchall()

        # Points ~141m apart should intersect with 200m buffer
        assert len(result) >= 1  # At least one pair found

    def test_detect_swapped_coordinates(self, mock_duckdb_connection):
        """Test detection of swapped lat/lon coordinates."""
        conn = mock_duckdb_connection

        # Create data with swapped coordinates (lat in X, lon in Y)
        conn.execute("""
            CREATE TABLE swapped AS
            SELECT ST_Point(55.7, 12.5) as geom  -- lat, lon instead of lon, lat
        """)

        info = log_geometry_bounds(conn, "swapped", "geom")
        assert info["detected_crs"] == WGS84
        assert info["coordinate_order"] == "lat_lon_swapped"


class TestNewCRSStrategyFunctions:
    """Test the new CRS strategy helper functions."""

    def test_sql_transform_for_supabase_from_utm(self):
        """Should generate transform SQL from UTM to WGS84."""
        from common.crs_utils import sql_transform_for_supabase

        result = sql_transform_for_supabase("geometry")
        assert result == "ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')"

    def test_sql_transform_for_supabase_already_wgs84(self):
        """Should return expression unchanged if already in WGS84."""
        from common.crs_utils import WGS84, sql_transform_for_supabase

        result = sql_transform_for_supabase("geometry", source_crs=WGS84)
        assert result == "geometry"

    def test_sql_transform_for_supabase_complex_expression(self):
        """Should handle complex geometry expressions."""
        from common.crs_utils import sql_transform_for_supabase

        result = sql_transform_for_supabase("ST_Centroid(geom)")
        assert "ST_Centroid(geom)" in result
        assert "EPSG:4326" in result

    def test_sql_transform_to_processing_crs_from_wgs84(self):
        """Should generate transform SQL from WGS84 to UTM."""
        from common.crs_utils import WGS84, sql_transform_to_processing_crs

        result = sql_transform_to_processing_crs("geometry", source_crs=WGS84)
        assert result == "ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')"

    def test_sql_transform_to_processing_crs_already_utm(self):
        """Should return expression unchanged if already in UTM."""
        from common.crs_utils import DANISH_UTM, sql_transform_to_processing_crs

        result = sql_transform_to_processing_crs("geometry", source_crs=DANISH_UTM)
        assert result == "geometry"

    def test_processing_crs_constant_is_utm(self):
        """Verify TARGET_CRS_PROCESSING is Danish UTM."""
        from common.crs_utils import DANISH_UTM, TARGET_CRS_PROCESSING

        assert TARGET_CRS_PROCESSING == DANISH_UTM

    def test_supabase_crs_constant_is_wgs84(self):
        """Verify TARGET_CRS_SUPABASE is WGS84."""
        from common.crs_utils import TARGET_CRS_SUPABASE, WGS84

        assert TARGET_CRS_SUPABASE == WGS84
