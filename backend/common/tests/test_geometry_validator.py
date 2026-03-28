"""Tests for shared geometry validation utilities."""

import pytest
from common.geometry_validator import (
    validate_and_normalize_to_utm,
    validate_and_transform_geometries_duckdb,
    verify_spatial_join_usage,
)


class TestValidateAndNormalizeToUtm:
    """Tests for validate_and_normalize_to_utm function."""

    def test_utm_data_no_transform(self, mock_duckdb_connection):
        """Data already in EPSG:25832 should remain unchanged."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_utm AS
            SELECT ST_GeomFromText('POLYGON((723000 6176000, 724000 6176000, 724000 6177000, 723000 6177000, 723000 6176000))') AS geometry
        """)

        validate_and_normalize_to_utm(conn, "test_utm", "test_dataset")

        bounds = conn.execute("""
            SELECT ST_XMin(geometry), ST_YMin(geometry) FROM test_utm
        """).fetchone()
        # Should still be in UTM range
        assert 400000 <= bounds[0] <= 900000
        assert 6000000 <= bounds[1] <= 6500000

    def test_wgs84_lon_lat_transforms_to_utm(self, mock_duckdb_connection):
        """WGS84 lon/lat data should be transformed to EPSG:25832."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_wgs84 AS
            SELECT ST_GeomFromText('POINT(12.5683 55.6761)') AS geometry
        """)

        validate_and_normalize_to_utm(conn, "test_wgs84", "test_dataset")

        bounds = conn.execute("""
            SELECT ST_XMin(geometry), ST_YMin(geometry) FROM test_wgs84
        """).fetchone()
        # Should now be in UTM range
        assert 400000 <= bounds[0] <= 900000
        assert 6000000 <= bounds[1] <= 6500000

    def test_wgs84_lat_lon_swapped_transforms_to_utm(self, mock_duckdb_connection):
        """WGS84 lat/lon (swapped) data should be flipped and transformed to EPSG:25832."""
        conn = mock_duckdb_connection
        # Swapped: X=lat (55.67), Y=lon (12.56)
        conn.execute("""
            CREATE TABLE test_swapped AS
            SELECT ST_GeomFromText('POINT(55.6761 12.5683)') AS geometry
        """)

        validate_and_normalize_to_utm(conn, "test_swapped", "test_dataset")

        bounds = conn.execute("""
            SELECT ST_XMin(geometry), ST_YMin(geometry) FROM test_swapped
        """).fetchone()
        # Should now be in UTM range after flip + transform
        assert 400000 <= bounds[0] <= 900000
        assert 6000000 <= bounds[1] <= 6500000

    def test_empty_table(self, mock_duckdb_connection):
        """Empty table should return gracefully."""
        conn = mock_duckdb_connection
        conn.execute("CREATE TABLE test_empty (geometry GEOMETRY)")

        # Should not raise
        validate_and_normalize_to_utm(conn, "test_empty", "test_dataset")

        count = conn.execute("SELECT COUNT(*) FROM test_empty").fetchone()[0]
        assert count == 0

    def test_null_geometries_removed(self, mock_duckdb_connection):
        """Null geometries should be removed."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_nulls AS
            SELECT ST_GeomFromText('POINT(723626 6176067)') AS geometry
            UNION ALL
            SELECT NULL AS geometry
        """)

        validate_and_normalize_to_utm(conn, "test_nulls", "test_dataset")

        count = conn.execute("SELECT COUNT(*) FROM test_nulls").fetchone()[0]
        assert count == 1

    def test_varchar_conversion(self, mock_duckdb_connection):
        """VARCHAR geometry columns should be converted to spatial type."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_varchar AS
            SELECT 'POINT(723626 6176067)' AS geometry
        """)

        validate_and_normalize_to_utm(conn, "test_varchar", "test_dataset")

        # Should have been converted and still have 1 row
        count = conn.execute("SELECT COUNT(*) FROM test_varchar").fetchone()[0]
        assert count == 1

        # Verify it's now a spatial type
        geom_type = conn.execute("""
            SELECT typeof(geometry) FROM test_varchar LIMIT 1
        """).fetchone()[0]
        assert geom_type != "VARCHAR"

    def test_missing_geometry_column_raises(self, mock_duckdb_connection):
        """Missing geometry column should raise ValueError."""
        conn = mock_duckdb_connection
        conn.execute("CREATE TABLE test_no_geom AS SELECT 1 AS id")

        with pytest.raises(ValueError, match="not found"):
            validate_and_normalize_to_utm(conn, "test_no_geom", "test_dataset")


class TestValidateAndTransformGeometriesDuckdb:
    """Tests for validate_and_transform_geometries_duckdb function."""

    def test_delegates_to_utm_when_target_is_25832(self, mock_duckdb_connection):
        """When target_crs is EPSG:25832, should delegate to validate_and_normalize_to_utm."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_delegate AS
            SELECT ST_GeomFromText('POINT(723626 6176067)') AS geometry
        """)

        validate_and_transform_geometries_duckdb(conn, "test_delegate", "test_dataset", target_crs="EPSG:25832")

        bounds = conn.execute("""
            SELECT ST_XMin(geometry), ST_YMin(geometry) FROM test_delegate
        """).fetchone()
        # Should remain in UTM
        assert 400000 <= bounds[0] <= 900000
        assert 6000000 <= bounds[1] <= 6500000

    def test_utm_to_wgs84(self, mock_duckdb_connection):
        """UTM data should be transformed to WGS84 when target is EPSG:4326."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE test_to_wgs84 AS
            SELECT ST_GeomFromText('POINT(723626 6176067)') AS geometry
        """)

        validate_and_transform_geometries_duckdb(conn, "test_to_wgs84", "test_dataset", target_crs="EPSG:4326")

        bounds = conn.execute("""
            SELECT ST_XMin(geometry), ST_YMin(geometry) FROM test_to_wgs84
        """).fetchone()
        # Should now be in WGS84 range (lon ~12.5, lat ~55.7)
        assert -180 <= bounds[0] <= 180
        assert -90 <= bounds[1] <= 90

    def test_empty_table(self, mock_duckdb_connection):
        """Empty table should return gracefully."""
        conn = mock_duckdb_connection
        conn.execute("CREATE TABLE test_empty_wgs84 (geometry GEOMETRY)")

        # Should not raise
        validate_and_transform_geometries_duckdb(conn, "test_empty_wgs84", "test_dataset")

    def test_missing_column_raises(self, mock_duckdb_connection):
        """Missing geometry column should raise ValueError."""
        conn = mock_duckdb_connection
        conn.execute("CREATE TABLE test_no_geom2 AS SELECT 1 AS id")

        with pytest.raises(ValueError, match="not found"):
            validate_and_transform_geometries_duckdb(conn, "test_no_geom2", "test_dataset")


class TestVerifySpatialJoinUsage:
    """Tests for verify_spatial_join_usage function."""

    def test_returns_bool(self, mock_duckdb_connection):
        """Should return a boolean value."""
        conn = mock_duckdb_connection
        conn.execute("""
            CREATE TABLE t1 (id INT, geometry GEOMETRY);
            CREATE TABLE t2 (id INT, geometry GEOMETRY);
        """)

        result = verify_spatial_join_usage(conn, "SELECT * FROM t1 JOIN t2 ON ST_Intersects(t1.geometry, t2.geometry)")
        assert isinstance(result, bool)

    def test_invalid_query_returns_false(self, mock_duckdb_connection):
        """Invalid query should return False without raising."""
        conn = mock_duckdb_connection
        result = verify_spatial_join_usage(conn, "INVALID SQL")
        assert result is False
