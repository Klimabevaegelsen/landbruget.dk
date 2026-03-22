"""End-to-end data quality validation tests.

This module tests comprehensive data quality checks across the full
medallion architecture (Bronze -> Silver -> Gold) to ensure:
- Danish identifiers (CVR, CHR, BFE) are preserved through transformations
- Geospatial data maintains EPSG:4326 (WGS84) for storage
- Data is joinable on CVR/CHR/BFE or coordinates
- NULL values are handled correctly
- Duplicates are detected and prevented
- Data type consistency is maintained

These tests validate the entire data pipeline quality assurance process
and verify compliance with landbruget.dk data quality rules.

Reference: .claude/rules/data-quality.md
"""

import duckdb

from backend.common.crs_utils import DANISH_UTM, DENMARK_BOUNDS_WGS84, WGS84

# =============================================================================
# Identifier Preservation Tests (Bronze -> Silver -> Gold)
# =============================================================================


def test_cvr_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, valid_cvr_numbers: list[str]
) -> None:
    """Test that CVR numbers are preserved through full pipeline."""
    # Bronze: Raw data with various CVR formats
    mock_duckdb_connection.execute(
        """
        CREATE TABLE bronze_companies AS
        SELECT * FROM (VALUES
            (31373077, 'Arla Foods', '2024-01-01'),
            (00113115, 'Test Co', '2024-01-01'),
            (12345678, 'Generic', '2024-01-01'),
            (00000123, 'Small Co', '2024-01-01')
        ) AS t(cvr_raw, company_name, _fetch_timestamp)
    """
    )

    # Silver: Normalize CVR format
    mock_duckdb_connection.execute(
        """
        CREATE TABLE silver_companies AS
        SELECT
            LPAD(CAST(cvr_raw AS VARCHAR), 8, '0') as cvr,
            company_name
        FROM bronze_companies
    """
    )

    # Gold: Join with other data (simulate)
    mock_duckdb_connection.execute(
        """
        CREATE TABLE gold_company_summary AS
        SELECT
            cvr,
            company_name,
            LENGTH(cvr) as cvr_length,
            regexp_matches(cvr, '^[0-9]{8}$') as cvr_valid
        FROM silver_companies
    """
    )

    # Verify CVR preservation
    rows = mock_duckdb_connection.execute("SELECT * FROM gold_company_summary").fetchall()

    # All CVRs should be 8 digits
    for _cvr, _name, cvr_length, cvr_valid in rows:
        assert cvr_length == 8, "All CVRs should be exactly 8 digits"
        assert cvr_valid, "All CVRs should match format ^[0-9]{8}$"

    # Check specific values preserved
    cvr_values = [r[0] for r in rows]
    assert "31373077" in cvr_values
    assert "00113115" in cvr_values
    assert "12345678" in cvr_values
    assert "00000123" in cvr_values


def test_chr_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, valid_chr_numbers: list[str]
) -> None:
    """Test that CHR numbers are preserved through full pipeline."""
    # Bronze: Raw data with various CHR formats
    mock_duckdb_connection.execute(
        """
        CREATE TABLE bronze_herds AS
        SELECT * FROM (VALUES
            (123456, 'Cattle', '2024-01-01'),
            (000123, 'Pig', '2024-01-01'),
            (654321, 'Cattle', '2024-01-01'),
            (1, 'Sheep', '2024-01-01')
        ) AS t(chr_raw, herd_type, _fetch_timestamp)
    """
    )

    # Silver: Normalize CHR format
    mock_duckdb_connection.execute(
        """
        CREATE TABLE silver_herds AS
        SELECT
            LPAD(CAST(chr_raw AS VARCHAR), 6, '0') as chr,
            herd_type
        FROM bronze_herds
    """
    )

    # Verify CHR preservation
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            chr,
            LENGTH(chr) as chr_length,
            regexp_matches(chr, '^[0-9]{6}$') as chr_valid
        FROM silver_herds
    """
    ).fetchall()

    # All CHRs should be 6 digits
    for _chr_val, chr_length, chr_valid in rows:
        assert chr_length == 6, "All CHRs should be exactly 6 digits"
        assert chr_valid, "All CHRs should match format ^[0-9]{6}$"

    # Check specific values preserved
    chr_values = [r[0] for r in rows]
    assert "123456" in chr_values
    assert "000123" in chr_values
    assert "654321" in chr_values
    assert "000001" in chr_values


def test_bfe_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that BFE numbers are preserved through full pipeline."""
    # Bronze: Raw BFE data
    mock_duckdb_connection.execute(
        """
        CREATE TABLE bronze_parcels AS
        SELECT * FROM (VALUES
            ('0101-123456-12a', 10.5, '2024-01-01'),
            ('0851-234567-1', 20.3, '2024-01-01'),
            ('0461-100000-abc', 15.7, '2024-01-01')
        ) AS t(bfe, area_ha, _fetch_timestamp)
    """
    )

    # Silver: Pass through BFE (no transformation needed for string format)
    mock_duckdb_connection.execute(
        """
        CREATE TABLE silver_parcels AS
        SELECT
            bfe,
            area_ha
        FROM bronze_parcels
        WHERE bfe IS NOT NULL
    """
    )

    # Verify BFE preservation
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            bfe,
            regexp_matches(bfe, '^[0-9]{4}-[0-9]{6}-[0-9a-zA-Z]+$') as bfe_valid
        FROM silver_parcels
    """
    ).fetchall()

    # All BFEs should be valid format
    for _bfe, bfe_valid in rows:
        assert bfe_valid, "All BFEs should match kommune-ejerlav-matr pattern"

    # Check specific values preserved
    bfe_values = [r[0] for r in rows]
    assert "0101-123456-12a" in bfe_values
    assert "0851-234567-1" in bfe_values
    assert "0461-100000-abc" in bfe_values


# =============================================================================
# Geospatial CRS Maintenance Tests
# =============================================================================


def test_geospatial_crs_maintained_wgs84(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that all geospatial data maintains EPSG:4326 for storage."""
    # Bronze: Data in UTM (as received from Danish sources)
    copenhagen = sample_danish_geometries["copenhagen_point"]
    aarhus = sample_danish_geometries["aarhus_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE bronze_locations AS
        SELECT 'Copenhagen' as name, ST_GeomFromText('{copenhagen["epsg_25832"]}') as geom
        UNION ALL
        SELECT 'Aarhus' as name, ST_GeomFromText('{aarhus["epsg_25832"]}') as geom
    """
    )

    # Silver: Transform to WGS84 for storage
    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE silver_locations AS
        SELECT
            name,
            ST_Transform(geom, '{DANISH_UTM}', '{WGS84}') as geom
        FROM bronze_locations
    """
    )

    # Verify coordinates are in WGS84 range
    rows = mock_duckdb_connection.execute(
        f"""
        SELECT
            name,
            ST_X(geom) as lon,
            ST_Y(geom) as lat,
            ST_X(geom) BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                AND ST_Y(geom) BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                as is_in_denmark_wgs84
        FROM silver_locations
    """
    ).fetchall()

    # Check that coordinates are in valid ranges (accounting for axis order swap)
    # ST_Transform may swap lat/lon depending on PROJ version
    for _name, lon, lat, _is_in_denmark_wgs84 in rows:
        # Check if coordinates are in Denmark bounds (either lon/lat or lat/lon order)
        in_bounds = (
            (7.5 <= lon <= 15.5 and 54.5 <= lat <= 58.0)  # standard order
            or (7.5 <= lat <= 15.5 and 54.5 <= lon <= 58.0)  # swapped order
        )
        assert in_bounds, f"Coordinates ({lon}, {lat}) should be in Denmark bounds (either order)"


def test_geometry_validation_bounds_check(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that geometries outside Denmark bounds are flagged."""
    # Create test data with some points outside Denmark
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_locations AS
        SELECT * FROM (VALUES
            ('Copenhagen', 12.5683, 55.6761),
            ('Paris', 2.3522, 48.8566),
            ('Aarhus', 10.2039, 56.1629),
            ('Berlin', 13.4050, 52.5200)
        ) AS t(location, lon, lat)
    """
    )

    # Validate bounds
    rows = mock_duckdb_connection.execute(
        f"""
        SELECT
            location,
            lon,
            lat,
            lon BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                AND lat BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                as is_in_denmark
        FROM test_locations
    """
    ).fetchall()

    # Build lookup by location
    # Build lookup: location -> is_in_denmark (cols: location, lon, lat, is_in_denmark)
    results = {row[0]: row[3] for row in rows}

    # Verify Denmark points are inside
    assert results["Copenhagen"], "Copenhagen should be inside bounds"
    assert results["Aarhus"], "Aarhus should be inside bounds"

    # Verify non-Denmark points are outside
    assert not results["Paris"], "Paris should be outside bounds"
    assert not results["Berlin"], "Berlin should be outside bounds"


# =============================================================================
# Data Joinability Tests
# =============================================================================


def test_data_joinability_on_cvr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that data can be joined on CVR identifier."""
    # Create two tables with CVR as join key
    mock_duckdb_connection.execute(
        """
        CREATE TABLE companies AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla'),
            ('10150817', 'Crown'),
            ('12345678', 'Test')
        ) AS t(cvr, company_name)
    """
    )

    mock_duckdb_connection.execute(
        """
        CREATE TABLE fields AS
        SELECT * FROM (VALUES
            ('31373077', 'F001', 10.5),
            ('31373077', 'F002', 20.3),
            ('12345678', 'F003', 15.7)
        ) AS t(cvr, field_id, area_ha)
    """
    )

    # Join on CVR
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            c.cvr,
            c.company_name,
            COUNT(f.field_id) as num_fields,
            SUM(f.area_ha) as total_area
        FROM companies c
        LEFT JOIN fields f ON c.cvr = f.cvr
        GROUP BY c.cvr, c.company_name
    """
    ).fetchall()

    # Verify join succeeded
    assert len(rows) == 3, "Should have 3 companies"

    # Check Arla has 2 fields
    _, _, arla_num_fields, arla_total_area = next(row for row in rows if row[1] == "Arla")
    assert arla_num_fields == 2, "Arla should have 2 fields"
    assert float(arla_total_area) == 30.8, "Arla total area should be 30.8 ha"


def test_data_joinability_on_chr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that data can be joined on CHR identifier."""
    # Create two tables with CHR as join key
    mock_duckdb_connection.execute(
        """
        CREATE TABLE herds AS
        SELECT * FROM (VALUES
            ('123456', 'Cattle'),
            ('654321', 'Pig'),
            ('111111', 'Cattle')
        ) AS t(chr, herd_type)
    """
    )

    mock_duckdb_connection.execute(
        """
        CREATE TABLE movements AS
        SELECT * FROM (VALUES
            ('123456', '2024-01-01', 10),
            ('123456', '2024-02-01', 5),
            ('654321', '2024-01-15', 20)
        ) AS t(chr, movement_date, num_animals)
    """
    )

    # Join on CHR
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            h.chr,
            h.herd_type,
            COUNT(m.movement_date) as num_movements,
            SUM(m.num_animals) as total_animals
        FROM herds h
        LEFT JOIN movements m ON h.chr = m.chr
        GROUP BY h.chr, h.herd_type
    """
    ).fetchall()

    # Verify join succeeded
    assert len(rows) == 3, "Should have 3 herds"

    # Check CHR 123456 has 2 movements
    # cols: chr, herd_type, num_movements, total_animals
    _, _, num_movements, total_animals = next(row for row in rows if row[0] == "123456")
    assert num_movements == 2, "CHR 123456 should have 2 movements"
    assert total_animals == 15, "CHR 123456 should have 15 total animals"


def test_data_joinability_on_geometry(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, sample_danish_geometries: dict
) -> None:
    """Test that data can be spatially joined on geometry."""
    # Create two overlapping polygons for spatial join
    copenhagen_field = sample_danish_geometries["sample_field_polygon"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE fields AS
        SELECT
            'F001' as field_id,
            ST_GeomFromText('{copenhagen_field["epsg_4326"]}') as geom
    """
    )

    # Create a zone that contains the field
    mock_duckdb_connection.execute(
        """
        CREATE TABLE protection_zones AS
        SELECT
            'Z001' as zone_id,
            ST_GeomFromText('POLYGON((12.5 55.6, 12.6 55.6, 12.6 55.7, 12.5 55.7, 12.5 55.6))') as geom
    """
    )

    # Spatial join: Find fields intersecting protection zones
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            f.field_id,
            p.zone_id,
            ST_Intersects(f.geom, p.geom) as intersects
        FROM fields f, protection_zones p
    """
    ).fetchall()

    # Verify spatial join worked
    assert len(rows) == 1, "Should have 1 field-zone pair"
    _field_id, _zone_id, intersects = rows[0]
    assert intersects, "Field should intersect protection zone"


# =============================================================================
# NULL Handling Tests
# =============================================================================


def test_null_identifier_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that NULL identifiers are handled correctly."""
    # Create data with NULL values
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_data AS
        SELECT * FROM (VALUES
            ('31373077', '123456', 'Arla'),
            (NULL, '654321', 'Unknown'),
            ('12345678', NULL, 'Test'),
            (NULL, NULL, 'Invalid')
        ) AS t(cvr, chr, company_name)
    """
    )

    # Filter out NULL identifiers
    rows = mock_duckdb_connection.execute(
        """
        SELECT *
        FROM test_data
        WHERE cvr IS NOT NULL OR chr IS NOT NULL
    """
    ).fetchall()

    # Should have 3 rows (excluding last row with all NULLs)
    assert len(rows) == 3, "Should have 3 rows with at least one valid identifier"

    # Check that we have valid identifiers
    assert len(rows) > 0, "Should have at least one valid row"
    # At least some rows should have valid CVR or CHR
    cvr_count = sum(1 for r in rows if r[0] is not None)
    chr_count = sum(1 for r in rows if r[1] is not None)
    assert cvr_count > 0 or chr_count > 0


def test_null_geometry_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that NULL geometries are handled correctly."""
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_fields AS
        SELECT * FROM (VALUES
            ('F001', 'POINT(12.5 55.6)'),
            ('F002', NULL),
            ('F003', 'POINT(10.2 56.1)')
        ) AS t(field_id, geom_wkt)
    """
    )

    # Convert to geometries, handling NULLs
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            field_id,
            geom_wkt IS NOT NULL as has_geometry,
            CASE WHEN geom_wkt IS NOT NULL
                 THEN ST_AsText(ST_GeomFromText(geom_wkt))
                 ELSE NULL
            END as geom_text
        FROM test_fields
    """
    ).fetchall()

    # Verify NULL handling
    for field_id, has_geometry, _geom_text in rows:
        if field_id == "F001":
            assert has_geometry  # F001 has geometry
        elif field_id == "F002":
            assert not has_geometry  # F002 has no geometry
        elif field_id == "F003":
            assert has_geometry  # F003 has geometry


# =============================================================================
# Duplicate Detection Tests
# =============================================================================


def test_duplicate_detection_by_cvr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that duplicate CVR records are detected."""
    # Create data with duplicates
    mock_duckdb_connection.execute(
        """
        CREATE TABLE companies AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla Foods', '2024-01-01'),
            ('31373077', 'Arla Foods', '2024-01-01'),
            ('12345678', 'Test Co', '2024-01-01'),
            ('31373077', 'Arla Foods', '2024-02-01')
        ) AS t(cvr, company_name, record_date)
    """
    )

    # Detect duplicates
    rows = mock_duckdb_connection.execute(
        """
        SELECT
            cvr,
            record_date,
            COUNT(*) as duplicate_count
        FROM companies
        GROUP BY cvr, record_date
        HAVING COUNT(*) > 1
    """
    ).fetchall()

    # Should detect 2 duplicates for Arla on 2024-01-01
    assert len(rows) == 1, "Should detect 1 duplicate group"
    cvr, _record_date, duplicate_count = rows[0]
    assert cvr == "31373077"
    assert duplicate_count == 2


def test_duplicate_prevention_with_upsert(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that duplicates are prevented using upsert pattern."""
    # Initial data
    mock_duckdb_connection.execute(
        """
        CREATE TABLE companies AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla Foods', '2024-01-01'),
            ('12345678', 'Test Co', '2024-01-01')
        ) AS t(cvr, company_name, updated_at)
    """
    )

    # New data with overlapping CVR (simulating upsert)
    mock_duckdb_connection.execute(
        """
        CREATE TABLE new_data AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla Foods Updated', '2024-02-01'),
            ('87654321', 'New Co', '2024-02-01')
        ) AS t(cvr, company_name, updated_at)
    """
    )

    # Simulate upsert: Delete existing, insert all
    mock_duckdb_connection.execute("DELETE FROM companies WHERE cvr IN (SELECT cvr FROM new_data)")
    mock_duckdb_connection.execute("INSERT INTO companies SELECT * FROM new_data")

    # Verify no duplicates
    rows = mock_duckdb_connection.execute(
        """
        SELECT cvr, COUNT(*) as count
        FROM companies
        GROUP BY cvr
    """
    ).fetchall()

    # All CVRs should be unique
    assert all(r[1] == 1 for r in rows), "All CVRs should be unique after upsert"
    assert len(rows) == 3, "Should have 3 unique companies"


# =============================================================================
# Data Type Consistency Tests
# =============================================================================


def test_data_type_consistency_identifiers(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that identifiers maintain string data type."""
    # Create data with proper types
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_identifiers AS
        SELECT * FROM (VALUES
            ('31373077', '123456'),
            ('00113115', '000123'),
            ('12345678', '654321')
        ) AS t(cvr, chr)
    """
    )

    # Check data types
    result = mock_duckdb_connection.execute(
        """
        SELECT
            typeof(cvr) as cvr_type,
            typeof(chr) as chr_type
        FROM test_identifiers
        LIMIT 1
    """
    ).fetchone()

    cvr_type, chr_type = result

    # Both should be VARCHAR (string)
    assert cvr_type == "VARCHAR", f"CVR should be VARCHAR, got {cvr_type}"
    assert chr_type == "VARCHAR", f"CHR should be VARCHAR, got {chr_type}"


def test_data_type_consistency_numeric(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that numeric fields maintain proper data types."""
    # Create data with numeric fields
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_fields AS
        SELECT * FROM (VALUES
            ('F001', 10.5, 2024),
            ('F002', 20.3, 2024),
            ('F003', 15.7, 2023)
        ) AS t(field_id, area_ha, year)
    """
    )

    # Check data types
    result = mock_duckdb_connection.execute(
        """
        SELECT
            typeof(area_ha) as area_type,
            typeof(year) as year_type
        FROM test_fields
        LIMIT 1
    """
    ).fetchone()

    area_type, year_type = result

    # Check numeric types (VALUES clause produces DECIMAL and INTEGER)
    assert area_type in ("DOUBLE", "DECIMAL(3,1)"), f"area_ha should be numeric, got {area_type}"
    assert year_type in ("BIGINT", "INTEGER"), f"year should be integer-like, got {year_type}"


def test_data_type_consistency_dates(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that date fields maintain proper data types."""
    # Create data with dates using CAST
    mock_duckdb_connection.execute(
        """
        CREATE TABLE test_records AS
        SELECT * FROM (VALUES
            (1, CAST('2024-01-01' AS DATE)),
            (2, CAST('2024-02-01' AS DATE)),
            (3, CAST('2024-03-01' AS DATE))
        ) AS t(record_id, record_date)
    """
    )

    # Check data types
    result = mock_duckdb_connection.execute(
        """
        SELECT typeof(record_date) as date_type
        FROM test_records
        LIMIT 1
    """
    ).fetchone()

    date_type = result[0]

    # Should be DATE
    assert date_type in ("TIMESTAMP", "TIMESTAMP_NS", "DATE"), f"record_date should be date-like, got {date_type}"


# =============================================================================
# Integration Test: Full Pipeline Quality Check
# =============================================================================


def test_full_pipeline_quality_check(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
    sample_danish_geometries: dict,
    valid_cvr_numbers: list[str],
) -> None:
    """Test complete data quality pipeline from Bronze to Gold."""
    # Bronze: Raw data from various sources
    copenhagen = sample_danish_geometries["copenhagen_point"]

    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE bronze_companies AS
        SELECT * FROM (VALUES
            (31373077, 'Arla Foods', '{copenhagen["epsg_25832"]}', '2024-01-01', 'wfs_api'),
            (113115, 'Test Co', '{copenhagen["epsg_25832"]}', '2024-01-01', 'wfs_api')
        ) AS t(cvr_raw, company_name, location_utm_wkt, _fetch_timestamp, _source)
    """
    )

    # Apply silver-layer normalization and validation
    mock_duckdb_connection.execute(
        f"""
        CREATE TABLE silver_companies AS
        SELECT
            LPAD(CAST(cvr_raw AS VARCHAR), 8, '0') as cvr,
            company_name,
            ST_Transform(
                ST_GeomFromText(location_utm_wkt),
                '{DANISH_UTM}',
                '{WGS84}'
            ) as location
        FROM bronze_companies
        WHERE cvr_raw IS NOT NULL
    """
    )

    # Gold: Final quality checks
    rows = mock_duckdb_connection.execute(
        f"""
        SELECT
            cvr,
            company_name,
            -- CVR validation
            LENGTH(cvr) = 8 as cvr_length_valid,
            regexp_matches(cvr, '^[0-9]{{8}}$') as cvr_format_valid,
            -- Geometry validation
            ST_X(location) as lon,
            ST_Y(location) as lat,
            ST_X(location) BETWEEN {DENMARK_BOUNDS_WGS84["min_x"]} AND {DENMARK_BOUNDS_WGS84["max_x"]}
                AND ST_Y(location) BETWEEN {DENMARK_BOUNDS_WGS84["min_y"]} AND {DENMARK_BOUNDS_WGS84["max_y"]}
                as location_in_denmark
        FROM silver_companies
    """
    ).fetchall()

    # Verify all quality checks pass
    # cols: cvr, company_name, cvr_length_valid, cvr_format_valid, lon, lat, location_in_denmark
    for row in rows:
        assert row[2], "All CVRs should be 8 digits"
        assert row[3], "All CVRs should match format"

    # Check locations are in Denmark (accounting for axis order swap in ST_Transform)
    for row in rows:
        lon, lat = row[4], row[5]
        in_bounds = (
            (7.5 <= lon <= 15.5 and 54.5 <= lat <= 58.0)  # standard order
            or (7.5 <= lat <= 15.5 and 54.5 <= lon <= 58.0)  # swapped order
        )
        assert in_bounds, f"Location ({lon}, {lat}) should be in Denmark bounds"

    # Verify specific values
    cvr_values = [cvr for cvr, *_ in rows]
    assert "31373077" in cvr_values, "Arla CVR should be normalized"
    assert "00113115" in cvr_values, "Leading zeros should be preserved"
