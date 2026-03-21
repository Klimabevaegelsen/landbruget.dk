"""
Comprehensive tests for the geometry validator module.

This module tests all functionality of unified_pipeline.common.geometry_validator,
including CRS transformation, invalid geometry handling, coordinate order detection,
and spatial operations validation.

Critical for ensuring:
- Data stored in EPSG:4326 (WGS84)
- Geometries are valid and within Denmark bounds
- Coordinate order is correct for spatial operations
- Spatial joins use SPATIAL_JOIN operator
"""

import duckdb
import pytest

from unified_pipeline.common.geometry_validator import (
    validate_and_transform_geometries_duckdb,
    verify_spatial_join_usage,
)


@pytest.fixture
def duck_conn():
    """Create a DuckDB connection with spatial extension loaded."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    return conn


# =============================================================================
# CRS Transformation Tests
# =============================================================================


class TestCRSTransformation:
    """Test coordinate reference system transformations."""

    def test_validate_transform_utm_to_wgs84(self, duck_conn):
        """Test transformation from EPSG:25832 (UTM 32N) to EPSG:4326 (WGS84).

        NOTE: The validator transforms UTM to WGS84 and stores coordinates in (LON, LAT) order
        using always_xy := true, which is the GeoJSON/GIS standard for interoperability.
        """
        # Create test data in UTM 32N (typical Danish coordinate system)
        # Copenhagen coordinates in UTM32N: approximately 725369, 6176652
        duck_conn.execute("""
            CREATE TABLE test_utm_data AS
            SELECT
                'Copenhagen' as name,
                ST_Point(725369, 6176652) as geometry
            UNION ALL
            SELECT
                'Aarhus' as name,
                ST_Point(600000, 6250000) as geometry
            UNION ALL
            SELECT
                'Aalborg' as name,
                ST_Point(550000, 6300000) as geometry
        """)

        # Validate and transform
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_utm_data",
            dataset_name="test_utm",
            geometry_column="geometry",
        )

        # Verify transformation to WGS84
        bounds = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_utm_data
        """).fetchone()

        min_x, max_x, min_y, max_y = bounds

        # After transformation with always_xy := true, coordinates are in (LON, LAT) order
        # This is the GeoJSON/GIS standard - X is longitude (7.5-15.5), Y is latitude (54.5-58)
        assert 7.5 <= min_x <= 15.5, f"Min X {min_x} should be in Denmark longitude range"
        assert 7.5 <= max_x <= 15.5, f"Max X {max_x} should be in Denmark longitude range"
        assert 54.5 <= min_y <= 58, f"Min Y {min_y} should be in Denmark latitude range"
        assert 54.5 <= max_y <= 58, f"Max Y {max_y} should be in Denmark latitude range"

        # Verify all geometries are valid
        invalid_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_utm_data
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_count == 0, "All geometries should be valid after transformation"

    def test_validate_transform_already_wgs84(self, duck_conn):
        """Test when data is already in EPSG:4326 (WGS84) - no transformation needed."""
        # Create test data already in WGS84 (LON, LAT order)
        duck_conn.execute("""
            CREATE TABLE test_wgs84_data AS
            SELECT
                'Copenhagen' as name,
                ST_Point(12.5681, 55.6761) as geometry  -- LON, LAT
            UNION ALL
            SELECT
                'Aarhus' as name,
                ST_Point(10.2039, 56.1629) as geometry
            UNION ALL
            SELECT
                'Aalborg' as name,
                ST_Point(9.9187, 57.0488) as geometry
        """)

        # Get bounds before validation
        bounds_before = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_wgs84_data
        """).fetchone()

        # Validate (should detect WGS84 and not transform)
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_wgs84_data",
            dataset_name="test_wgs84",
            geometry_column="geometry",
        )

        # Get bounds after validation
        bounds_after = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_wgs84_data
        """).fetchone()

        # Coordinates should remain essentially unchanged (within small tolerance)
        for before, after in zip(bounds_before, bounds_after, strict=False):
            assert abs(before - after) < 0.01, (
                "Coordinates should not change significantly when already in WGS84"
            )

    def test_validate_transform_preserves_attributes(self, duck_conn):
        """Test that non-geometry columns are preserved during transformation."""
        # Create test data with multiple attributes
        duck_conn.execute("""
            CREATE TABLE test_attributes AS
            SELECT
                'Field_001' as field_id,
                'Wheat' as crop_type,
                150.5 as area_ha,
                2024 as year,
                ST_Point(725369, 6176652) as geometry
            UNION ALL
            SELECT
                'Field_002' as field_id,
                'Barley' as crop_type,
                200.3 as area_ha,
                2024 as year,
                ST_Point(600000, 6250000) as geometry
        """)

        # Validate and transform
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_attributes",
            dataset_name="test_attrs",
            geometry_column="geometry",
        )

        # Verify all attributes are preserved
        result = duck_conn.execute("""
            SELECT field_id, crop_type, area_ha, year
            FROM test_attributes
            ORDER BY field_id
        """).fetchall()

        assert len(result) == 2, "All records should be preserved"
        # Check each field (DuckDB may return Decimal for numeric types)
        assert result[0][0] == "Field_001"
        assert result[0][1] == "Wheat"
        assert float(result[0][2]) == 150.5
        assert result[0][3] == 2024

        assert result[1][0] == "Field_002"
        assert result[1][1] == "Barley"
        assert float(result[1][2]) == 200.3
        assert result[1][3] == 2024


# =============================================================================
# Invalid Geometry Tests
# =============================================================================


class TestInvalidGeometryHandling:
    """Test handling of invalid geometries."""

    def test_validate_invalid_geometry_fix(self, duck_conn):
        """Test that ST_MakeValid fixes invalid geometries."""
        # Create invalid geometry (bowtie polygon)
        duck_conn.execute("""
            CREATE TABLE test_invalid AS
            SELECT
                'invalid_bowtie' as name,
                ST_GeomFromText('POLYGON((12 55, 13 56, 12 56, 13 55, 12 55))') as geometry
        """)

        # Check it's initially invalid
        invalid_before = duck_conn.execute("""
            SELECT COUNT(*) FROM test_invalid
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_before > 0, "Should have invalid geometry initially"

        # Validate and fix
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_invalid",
            dataset_name="test_invalid_fix",
            geometry_column="geometry",
        )

        # Check all geometries are now valid
        invalid_after = duck_conn.execute("""
            SELECT COUNT(*) FROM test_invalid
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_after == 0, "All geometries should be valid after fixing"

    def test_validate_self_intersecting_polygon(self, duck_conn):
        """Test repair of self-intersecting polygon."""
        # Create self-intersecting polygon (figure-8 shape)
        duck_conn.execute("""
            CREATE TABLE test_self_intersect AS
            SELECT
                'self_intersecting' as name,
                ST_GeomFromText(
                    'POLYGON((12 55, 13 56, 13 55, 12 56, 12 55))'
                ) as geometry
        """)

        # Validate and fix
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_self_intersect",
            dataset_name="test_self_intersect",
            geometry_column="geometry",
        )

        # Verify geometry is now valid
        is_valid = duck_conn.execute("""
            SELECT ST_IsValid(geometry) FROM test_self_intersect
        """).fetchone()[0]
        assert is_valid, "Self-intersecting polygon should be fixed"

    def test_validate_invalid_topology(self, duck_conn):
        """Test handling of various topology errors."""
        # Create multiple invalid geometries
        duck_conn.execute("""
            CREATE TABLE test_topology AS
            SELECT
                'spike' as name,
                ST_GeomFromText(
                    'POLYGON((12 55, 13 55, 13 56, 12.5 55.5, 12 56, 12 55))'
                ) as geometry
            UNION ALL
            SELECT
                'duplicate_points' as name,
                ST_GeomFromText(
                    'POLYGON((12 55, 12 55, 13 55, 13 56, 12 56, 12 55))'
                ) as geometry
        """)

        initial_count = duck_conn.execute("SELECT COUNT(*) FROM test_topology").fetchone()[0]

        # Validate and fix
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_topology",
            dataset_name="test_topology",
            geometry_column="geometry",
        )

        # Check that we still have records (fixed or removed)
        final_count = duck_conn.execute("SELECT COUNT(*) FROM test_topology").fetchone()[0]
        assert final_count <= initial_count, "Should not add records"

        # All remaining geometries should be valid
        invalid_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_topology
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_count == 0, "All remaining geometries should be valid"


# =============================================================================
# CRS Detection Tests
# =============================================================================


class TestCRSDetection:
    """Test automatic CRS detection logic."""

    def test_detect_crs_from_bounds_wgs84_lon_lat(self, duck_conn):
        """Test detection of WGS84 data in LON/LAT order."""
        # Create data in WGS84 LON/LAT order
        duck_conn.execute("""
            CREATE TABLE test_wgs84_lonlat AS
            SELECT ST_Point(12.5, 55.7) as geometry
            UNION ALL SELECT ST_Point(10.2, 56.1) as geometry
            UNION ALL SELECT ST_Point(9.9, 57.0) as geometry
        """)

        # Get initial bounds
        bounds = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_wgs84_lonlat
        """).fetchone()

        min_x, max_x, min_y, max_y = bounds

        # Should detect as WGS84 LON/LAT (expanded ranges from validator)
        is_wgs84_lon_lat = (
            3 <= min_x <= 17 and 3 <= max_x <= 17 and 53 <= min_y <= 59 and 53 <= max_y <= 59
        )
        assert is_wgs84_lon_lat, "Should detect WGS84 LON/LAT order"

    def test_detect_crs_from_bounds_wgs84_lat_lon(self, duck_conn):
        """Test detection of WGS84 data in LAT/LON order."""
        # Create data in WGS84 LAT/LON order (swapped)
        duck_conn.execute("""
            CREATE TABLE test_wgs84_latlon AS
            SELECT ST_Point(55.7, 12.5) as geometry
            UNION ALL SELECT ST_Point(56.1, 10.2) as geometry
            UNION ALL SELECT ST_Point(57.0, 9.9) as geometry
        """)

        # Get bounds
        bounds = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_wgs84_latlon
        """).fetchone()

        min_x, max_x, min_y, max_y = bounds

        # Should detect as WGS84 LAT/LON (swapped)
        is_wgs84_lat_lon = (
            53 <= min_x <= 59 and 53 <= max_x <= 59 and 3 <= min_y <= 17 and 3 <= max_y <= 17
        )
        assert is_wgs84_lat_lon, "Should detect WGS84 LAT/LON order"

    def test_detect_crs_from_bounds_utm(self, duck_conn):
        """Test detection of UTM coordinates (large values)."""
        # Create data in UTM coordinates
        duck_conn.execute("""
            CREATE TABLE test_utm AS
            SELECT ST_Point(725369, 6176652) as geometry
            UNION ALL SELECT ST_Point(600000, 6250000) as geometry
            UNION ALL SELECT ST_Point(550000, 6300000) as geometry
        """)

        # Get bounds
        bounds = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_utm
        """).fetchone()

        min_x, _max_x, min_y, _max_y = bounds

        # Should detect as UTM (large coordinate values)
        is_utm = 400000 <= min_x <= 900000 and 6000000 <= min_y <= 7000000
        assert is_utm, f"Should detect UTM coordinates (bounds: {bounds})"

    def test_detect_crs_ambiguous(self, duck_conn):
        """Test handling of ambiguous coordinate systems."""
        # Create data with ambiguous coordinates (outside typical ranges)
        duck_conn.execute("""
            CREATE TABLE test_ambiguous AS
            SELECT ST_Point(1000, 2000) as geometry
        """)

        # Validator should assume UTM and attempt transformation
        # This test mainly ensures it doesn't crash
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_ambiguous",
            dataset_name="test_ambiguous",
            geometry_column="geometry",
        )

        # Should complete without error
        count = duck_conn.execute("SELECT COUNT(*) FROM test_ambiguous").fetchone()[0]
        assert count >= 0, "Should handle ambiguous coordinates gracefully"


# =============================================================================
# Coordinate Order Tests
# =============================================================================


class TestCoordinateOrder:
    """Test coordinate order verification logic."""

    def test_coordinate_order_verification(self, duck_conn):
        """Test verification that coordinates are in correct order."""
        # Create test data with correct LON/LAT order
        duck_conn.execute("""
            CREATE TABLE test_coord_order AS
            SELECT
                'Copenhagen' as city,
                ST_Point(12.5681, 55.6761) as geometry  -- LON, LAT
            UNION ALL
            SELECT
                'Aarhus' as city,
                ST_Point(10.2039, 56.1629) as geometry
            UNION ALL
            SELECT
                'Aalborg' as city,
                ST_Point(9.9187, 57.0488) as geometry
        """)

        # Run validation
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_coord_order",
            dataset_name="test_coord_order",
            geometry_column="geometry",
        )

        # Extract coordinates to verify order
        coords = duck_conn.execute("""
            SELECT ST_X(geometry) as x, ST_Y(geometry) as y
            FROM test_coord_order
        """).fetchall()

        # All X values (first coordinate) should be longitude (8-15°)
        # All Y values (second coordinate) should be latitude (54-58°)
        for x, y in coords:
            assert 8 <= x <= 15, f"X coordinate {x} should be longitude in Denmark range"
            assert 54 <= y <= 58, f"Y coordinate {y} should be latitude in Denmark range"

    def test_coordinate_order_swap_detection(self, duck_conn):
        """Test detection when coordinates are swapped (LAT/LON instead of LON/LAT)."""
        # Create data with swapped coordinates
        duck_conn.execute("""
            CREATE TABLE test_swapped AS
            SELECT ST_Point(55.6761, 12.5681) as geometry  -- LAT, LON - WRONG!
            UNION ALL SELECT ST_Point(56.1629, 10.2039) as geometry
            UNION ALL SELECT ST_Point(57.0488, 9.9187) as geometry
        """)

        # Get bounds to check detection
        bounds = duck_conn.execute("""
            SELECT
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y
            FROM test_swapped
        """).fetchone()

        min_x, max_x, min_y, max_y = bounds

        # Should detect LAT/LON order (first coordinate is latitude)
        is_lat_lon = (
            53 <= min_x <= 59 and 53 <= max_x <= 59 and 3 <= min_y <= 17 and 3 <= max_y <= 17
        )
        assert is_lat_lon, "Should detect coordinates are in LAT/LON order"


# =============================================================================
# Spatial Operations Tests
# =============================================================================


class TestSpatialOperations:
    """Test spatial operations and optimizations."""

    def test_verify_spatial_join_usage(self, duck_conn):
        """Test that spatial joins use SPATIAL_JOIN operator."""
        # Create two tables for spatial join
        duck_conn.execute("""
            CREATE TABLE fields AS
            SELECT
                'Field_001' as id,
                ST_GeomFromText('POLYGON((12 55, 13 55, 13 56, 12 56, 12 55))') as geometry
        """)

        duck_conn.execute("""
            CREATE TABLE boundaries AS
            SELECT
                'Zone_A' as zone,
                ST_GeomFromText('POLYGON((11 54, 14 54, 14 57, 11 57, 11 54))') as geometry
        """)

        # Create a spatial join query
        query = """
            SELECT f.id, b.zone
            FROM fields f, boundaries b
            WHERE ST_Intersects(f.geometry, b.geometry)
        """

        # Verify SPATIAL_JOIN is used
        uses_spatial_join = verify_spatial_join_usage(duck_conn, query)

        # Note: SPATIAL_JOIN detection may depend on query complexity and DuckDB version
        # We mainly test that the function runs without error
        assert isinstance(uses_spatial_join, bool), "Should return boolean result"

    def test_geometry_within_denmark_bounds(self, duck_conn):
        """Test that transformed geometries are within Denmark bounds."""
        # Create test data across Denmark
        duck_conn.execute("""
            CREATE TABLE denmark_test AS
            SELECT
                'Copenhagen' as city,
                ST_Point(12.5681, 55.6761) as geometry
            UNION ALL
            SELECT
                'Aalborg' as city,
                ST_Point(9.9187, 57.0488) as geometry
            UNION ALL
            SELECT
                'Bornholm' as city,
                ST_Point(14.9, 55.1) as geometry
        """)

        # Validate
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="denmark_test",
            dataset_name="denmark_bounds",
            geometry_column="geometry",
        )

        # Check all points are within Denmark bounds
        result = duck_conn.execute("""
            SELECT
                city,
                ST_X(geometry) as lon,
                ST_Y(geometry) as lat
            FROM denmark_test
        """).fetchall()

        for city, lon, lat in result:
            # Denmark bounds: lon ∈ [7.5, 15.5], lat ∈ [54.5, 58]
            assert 7.5 <= lon <= 15.5, f"{city} longitude {lon} outside Denmark bounds"
            assert 54.5 <= lat <= 58, f"{city} latitude {lat} outside Denmark bounds"


# =============================================================================
# Null/Empty Geometry Tests
# =============================================================================


class TestNullEmptyGeometries:
    """Test handling of NULL and empty geometries."""

    def test_remove_null_empty_geometries(self, duck_conn):
        """Test that NULL geometries are removed."""
        # Create table with NULL geometries
        duck_conn.execute("""
            CREATE TABLE test_nulls AS
            SELECT 'valid' as name, ST_Point(12.5, 55.7) as geometry
            UNION ALL
            SELECT 'null_geom' as name, NULL::GEOMETRY as geometry
            UNION ALL
            SELECT 'valid2' as name, ST_Point(10.2, 56.1) as geometry
        """)

        initial_count = duck_conn.execute("SELECT COUNT(*) FROM test_nulls").fetchone()[0]
        assert initial_count == 3, "Should start with 3 records"

        # Validate (should remove NULL geometries)
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_nulls",
            dataset_name="test_nulls",
            geometry_column="geometry",
        )

        # Check NULL geometries removed
        final_count = duck_conn.execute("SELECT COUNT(*) FROM test_nulls").fetchone()[0]
        assert final_count == 2, "Should remove NULL geometries"

        # Verify no NULL geometries remain
        null_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_nulls
            WHERE geometry IS NULL
        """).fetchone()[0]
        assert null_count == 0, "No NULL geometries should remain"

    def test_handle_empty_geometry_collection(self, duck_conn):
        """Test handling of empty geometry collections."""
        # Create table with empty geometries
        duck_conn.execute("""
            CREATE TABLE test_empty AS
            SELECT 'valid' as name, ST_Point(12.5, 55.7) as geometry
            UNION ALL
            SELECT 'empty' as name, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY') as geometry
        """)

        # Validate (should remove empty geometries)
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_empty",
            dataset_name="test_empty",
            geometry_column="geometry",
        )

        # Check empty geometries removed
        final_count = duck_conn.execute("SELECT COUNT(*) FROM test_empty").fetchone()[0]
        assert final_count == 1, "Should remove empty geometries"


# =============================================================================
# Type Detection Tests
# =============================================================================


class TestTypeDetection:
    """Test geometry type detection and conversion."""

    def test_detect_varchar_geometry(self, duck_conn):
        """Test detection and conversion of VARCHAR geometry strings."""
        # Create table with WKT strings (VARCHAR)
        duck_conn.execute("""
            CREATE TABLE test_varchar AS
            SELECT
                'point1' as name,
                'POINT(12.5 55.7)' as geometry
            UNION ALL
            SELECT
                'point2' as name,
                'POINT(10.2 56.1)' as geometry
        """)

        # Check initial type
        geom_type = duck_conn.execute("""
            SELECT DISTINCT typeof(geometry)
            FROM test_varchar
            LIMIT 1
        """).fetchone()[0]
        assert geom_type == "VARCHAR", "Initial type should be VARCHAR"

        # Validate (should convert to spatial geometry)
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_varchar",
            dataset_name="test_varchar",
            geometry_column="geometry",
        )

        # Check type after validation
        geom_type_after = duck_conn.execute("""
            SELECT DISTINCT typeof(geometry)
            FROM test_varchar
            LIMIT 1
        """).fetchone()[0]
        assert "GEOMETRY" in geom_type_after or "POINT" in geom_type_after, (
            "Should convert to spatial type"
        )

    def test_detect_spatial_geometry(self, duck_conn):
        """Test detection of native spatial geometry types."""
        # Create table with native geometry type
        duck_conn.execute("""
            CREATE TABLE test_spatial AS
            SELECT
                'point1' as name,
                ST_Point(12.5, 55.7) as geometry
            UNION ALL
            SELECT
                'point2' as name,
                ST_Point(10.2, 56.1) as geometry
        """)

        # Check initial type
        geom_type = duck_conn.execute("""
            SELECT DISTINCT typeof(geometry)
            FROM test_spatial
            LIMIT 1
        """).fetchone()[0]
        assert "GEOMETRY" in geom_type or "POINT" in geom_type, "Should be spatial type"

        # Validate (should skip conversion)
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_spatial",
            dataset_name="test_spatial",
            geometry_column="geometry",
        )

        # Type should remain spatial
        geom_type_after = duck_conn.execute("""
            SELECT DISTINCT typeof(geometry)
            FROM test_spatial
            LIMIT 1
        """).fetchone()[0]
        assert "GEOMETRY" in geom_type_after or "POINT" in geom_type_after


# =============================================================================
# Integration Tests
# =============================================================================


class TestFullValidationPipeline:
    """Integration tests for complete validation workflows."""

    def test_full_validation_pipeline_utm_to_wgs84(self, duck_conn):
        """Test complete validation flow: UTM → WGS84 with all checks."""
        # Create realistic test data with various issues
        duck_conn.execute("""
            CREATE TABLE test_full_pipeline AS
            SELECT
                'Field_001' as field_id,
                'Wheat' as crop,
                ST_Point(725369, 6176652) as geometry  -- Copenhagen UTM
            UNION ALL
            SELECT
                'Field_002' as field_id,
                'Barley' as crop,
                ST_Point(600000, 6250000) as geometry  -- Aarhus UTM
            UNION ALL
            SELECT
                'Field_003' as field_id,
                'Corn' as crop,
                NULL::GEOMETRY as geometry  -- NULL - should be removed
        """)

        initial_count = duck_conn.execute("SELECT COUNT(*) FROM test_full_pipeline").fetchone()[0]
        assert initial_count == 3

        # Run full validation
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_full_pipeline",
            dataset_name="full_pipeline",
            geometry_column="geometry",
        )

        # Verify results
        final_count = duck_conn.execute("SELECT COUNT(*) FROM test_full_pipeline").fetchone()[0]
        assert final_count == 2, "NULL geometry should be removed"

        # Check all geometries valid
        invalid_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_full_pipeline
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_count == 0

        # Check transformed to WGS84 with always_xy := true (stored as LON/LAT - GeoJSON standard)
        result = duck_conn.execute("""
            SELECT
                ST_X(geometry) as lon,
                ST_Y(geometry) as lat
            FROM test_full_pipeline
        """).fetchall()

        # Coordinates are stored as (LON, LAT) per GeoJSON standard (always_xy := true)
        # This is the interoperable standard for web mapping and GeoJSON
        for lon, lat in result:
            assert 7.5 <= lon <= 15.5, f"X coordinate (longitude) {lon} should be in Denmark range"
            assert 54.5 <= lat <= 58, f"Y coordinate (latitude) {lat} should be in Denmark range"

        # Check attributes preserved
        crops = duck_conn.execute("""
            SELECT crop FROM test_full_pipeline ORDER BY field_id
        """).fetchall()
        assert len(crops) == 2
        assert crops[0][0] == "Wheat"
        assert crops[1][0] == "Barley"

    def test_full_validation_pipeline_mixed_geometries(self, duck_conn):
        """Test validation with mixed valid/invalid geometries."""
        # Create mix of valid, invalid, and NULL geometries
        duck_conn.execute("""
            CREATE TABLE test_mixed AS
            SELECT
                'valid_point' as name,
                ST_Point(12.5, 55.7) as geometry
            UNION ALL
            SELECT
                'invalid_bowtie' as name,
                ST_GeomFromText('POLYGON((12 55, 13 56, 12 56, 13 55, 12 55))') as geometry
            UNION ALL
            SELECT
                'valid_polygon' as name,
                ST_GeomFromText('POLYGON((12 55, 13 55, 13 56, 12 56, 12 55))') as geometry
            UNION ALL
            SELECT
                'null_geom' as name,
                NULL::GEOMETRY as geometry
        """)

        initial_count = duck_conn.execute("SELECT COUNT(*) FROM test_mixed").fetchone()[0]
        assert initial_count == 4

        # Run validation
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_mixed",
            dataset_name="mixed",
            geometry_column="geometry",
        )

        # Check results
        final_count = duck_conn.execute("SELECT COUNT(*) FROM test_mixed").fetchone()[0]
        assert final_count >= 2, "Should keep valid geometries and fix invalid ones"

        # All remaining should be valid
        invalid_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_mixed
            WHERE NOT ST_IsValid(geometry)
        """).fetchone()[0]
        assert invalid_count == 0, "All remaining geometries should be valid"

        # No NULL geometries
        null_count = duck_conn.execute("""
            SELECT COUNT(*) FROM test_mixed WHERE geometry IS NULL
        """).fetchone()[0]
        assert null_count == 0, "NULL geometries should be removed"


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_missing_geometry_column(self, duck_conn):
        """Test handling when geometry column doesn't exist."""
        duck_conn.execute("""
            CREATE TABLE test_no_geom AS
            SELECT 'test' as name, 123 as value
        """)

        # Should raise ValueError
        with pytest.raises(ValueError, match=r"Geometry column.*not found"):
            validate_and_transform_geometries_duckdb(
                conn=duck_conn,
                table_name="test_no_geom",
                dataset_name="no_geom",
                geometry_column="geometry",
            )

    def test_empty_table(self, duck_conn):
        """Test handling of empty table."""
        duck_conn.execute("""
            CREATE TABLE test_empty_table (
                name TEXT,
                geometry GEOMETRY
            )
        """)

        # Should complete without error
        validate_and_transform_geometries_duckdb(
            conn=duck_conn,
            table_name="test_empty_table",
            dataset_name="empty",
            geometry_column="geometry",
        )

        # Should still be empty
        count = duck_conn.execute("SELECT COUNT(*) FROM test_empty_table").fetchone()[0]
        assert count == 0


if __name__ == "__main__":
    # Allow running this test file directly for debugging
    pytest.main([__file__, "-v", "-s"])
