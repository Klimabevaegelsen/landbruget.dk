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
import pandas as pd
from backend.common.crs_utils import DANISH_UTM, DENMARK_BOUNDS_WGS84, WGS84

# =============================================================================
# Identifier Preservation Tests (Bronze -> Silver -> Gold)
# =============================================================================


def test_cvr_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, valid_cvr_numbers: list[str]
) -> None:
    """Test that CVR numbers are preserved through full pipeline."""
    # Bronze: Raw data with various CVR formats
    pd.DataFrame(
        {
            "cvr_raw": [31373077, "00113115", 12345678, "00000123"],
            "company_name": ["Arla Foods", "Test Co", "Generic", "Small Co"],
            "_fetch_timestamp": ["2024-01-01"] * 4,
        }
    )

    mock_duckdb_connection.execute(
        "CREATE TABLE bronze_companies AS SELECT * FROM bronze_df"
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
    result = mock_duckdb_connection.execute(
        "SELECT * FROM gold_company_summary"
    ).fetchdf()

    # All CVRs should be 8 digits
    assert all(result["cvr_length"] == 8), "All CVRs should be exactly 8 digits"

    # All CVRs should be valid format
    assert all(result["cvr_valid"]), "All CVRs should match format ^[0-9]{8}$"

    # Check specific values preserved
    assert "31373077" in result["cvr"].values
    assert "00113115" in result["cvr"].values
    assert "12345678" in result["cvr"].values
    assert "00000123" in result["cvr"].values


def test_chr_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection, valid_chr_numbers: list[str]
) -> None:
    """Test that CHR numbers are preserved through full pipeline."""
    # Bronze: Raw data with various CHR formats
    pd.DataFrame(
        {
            "chr_raw": [123456, "000123", 654321, 1],
            "herd_type": ["Cattle", "Pig", "Cattle", "Sheep"],
            "_fetch_timestamp": ["2024-01-01"] * 4,
        }
    )

    mock_duckdb_connection.execute(
        "CREATE TABLE bronze_herds AS SELECT * FROM bronze_df"
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
    result = mock_duckdb_connection.execute(
        """
        SELECT
            chr,
            LENGTH(chr) as chr_length,
            regexp_matches(chr, '^[0-9]{6}$') as chr_valid
        FROM silver_herds
    """
    ).fetchdf()

    # All CHRs should be 6 digits
    assert all(result["chr_length"] == 6), "All CHRs should be exactly 6 digits"

    # All CHRs should be valid format
    assert all(result["chr_valid"]), "All CHRs should match format ^[0-9]{6}$"

    # Check specific values preserved
    assert "123456" in result["chr"].values
    assert "000123" in result["chr"].values
    assert "654321" in result["chr"].values
    assert "000001" in result["chr"].values


def test_bfe_preservation_through_pipeline(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that BFE numbers are preserved through full pipeline."""
    # Bronze: Raw BFE data
    pd.DataFrame(
        {
            "bfe": ["0101-123456-12a", "0851-234567-1", "0461-100000-abc"],
            "area_ha": [10.5, 20.3, 15.7],
            "_fetch_timestamp": ["2024-01-01"] * 3,
        }
    )

    mock_duckdb_connection.execute(
        "CREATE TABLE bronze_parcels AS SELECT * FROM bronze_df"
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
    result = mock_duckdb_connection.execute(
        """
        SELECT
            bfe,
            regexp_matches(bfe, '^[0-9]{4}-[0-9]{6}-[0-9a-zA-Z]+$') as bfe_valid
        FROM silver_parcels
    """
    ).fetchdf()

    # All BFEs should be valid format
    assert all(result["bfe_valid"]), (
        "All BFEs should match kommune-ejerlav-matr pattern"
    )

    # Check specific values preserved
    assert "0101-123456-12a" in result["bfe"].values
    assert "0851-234567-1" in result["bfe"].values
    assert "0461-100000-abc" in result["bfe"].values


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
    result = mock_duckdb_connection.execute(
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
    ).fetchdf()

    # Check that coordinates are in valid ranges (accounting for axis order swap)
    # ST_Transform may swap lat/lon depending on PROJ version
    for _, row in result.iterrows():
        lon, lat = row["lon"], row["lat"]
        # Check if coordinates are in Denmark bounds (either lon/lat or lat/lon order)
        in_bounds = (
            (7.5 <= lon <= 15.5 and 54.5 <= lat <= 58.0)  # standard order
            or (7.5 <= lat <= 15.5 and 54.5 <= lon <= 58.0)  # swapped order
        )
        assert in_bounds, (
            f"Coordinates ({lon}, {lat}) should be in Denmark bounds (either order)"
        )


def test_geometry_validation_bounds_check(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that geometries outside Denmark bounds are flagged."""
    # Create test data with some points outside Denmark
    pd.DataFrame(
        {
            "location": ["Copenhagen", "Paris", "Aarhus", "Berlin"],
            "lon": [12.5683, 2.3522, 10.2039, 13.4050],
            "lat": [55.6761, 48.8566, 56.1629, 52.5200],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_locations AS SELECT * FROM df")

    # Validate bounds
    result = mock_duckdb_connection.execute(
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
    ).fetchdf()

    # Verify Denmark points are inside
    denmark_points = result[result["location"].isin(["Copenhagen", "Aarhus"])]
    assert all(denmark_points["is_in_denmark"]), "Danish points should be inside bounds"

    # Verify non-Denmark points are outside
    foreign_points = result[result["location"].isin(["Paris", "Berlin"])]
    assert not any(foreign_points["is_in_denmark"]), (
        "Foreign points should be outside bounds"
    )


# =============================================================================
# Data Joinability Tests
# =============================================================================


def test_data_joinability_on_cvr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that data can be joined on CVR identifier."""
    # Create two tables with CVR as join key
    pd.DataFrame(
        {
            "cvr": ["31373077", "10150817", "12345678"],
            "company_name": ["Arla", "Crown", "Test"],
        }
    )

    pd.DataFrame(
        {
            "cvr": ["31373077", "31373077", "12345678"],
            "field_id": ["F001", "F002", "F003"],
            "area_ha": [10.5, 20.3, 15.7],
        }
    )

    mock_duckdb_connection.execute(
        "CREATE TABLE companies AS SELECT * FROM companies_df"
    )
    mock_duckdb_connection.execute("CREATE TABLE fields AS SELECT * FROM fields_df")

    # Join on CVR
    result = mock_duckdb_connection.execute(
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
    ).fetchdf()

    # Verify join succeeded
    assert len(result) == 3, "Should have 3 companies"

    # Check Arla has 2 fields
    arla = result[result["company_name"] == "Arla"]
    assert arla["num_fields"].iloc[0] == 2, "Arla should have 2 fields"
    assert arla["total_area"].iloc[0] == 30.8, "Arla total area should be 30.8 ha"


def test_data_joinability_on_chr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that data can be joined on CHR identifier."""
    # Create two tables with CHR as join key
    pd.DataFrame(
        {
            "chr": ["123456", "654321", "111111"],
            "herd_type": ["Cattle", "Pig", "Cattle"],
        }
    )

    pd.DataFrame(
        {
            "chr": ["123456", "123456", "654321"],
            "movement_date": ["2024-01-01", "2024-02-01", "2024-01-15"],
            "num_animals": [10, 5, 20],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE herds AS SELECT * FROM herds_df")
    mock_duckdb_connection.execute(
        "CREATE TABLE movements AS SELECT * FROM movements_df"
    )

    # Join on CHR
    result = mock_duckdb_connection.execute(
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
    ).fetchdf()

    # Verify join succeeded
    assert len(result) == 3, "Should have 3 herds"

    # Check CHR 123456 has 2 movements
    herd_123456 = result[result["chr"] == "123456"]
    assert herd_123456["num_movements"].iloc[0] == 2, (
        "CHR 123456 should have 2 movements"
    )
    assert herd_123456["total_animals"].iloc[0] == 15, (
        "CHR 123456 should have 15 total animals"
    )


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
    result = mock_duckdb_connection.execute(
        """
        SELECT
            f.field_id,
            p.zone_id,
            ST_Intersects(f.geom, p.geom) as intersects
        FROM fields f, protection_zones p
    """
    ).fetchdf()

    # Verify spatial join worked
    assert len(result) == 1, "Should have 1 field-zone pair"
    assert result["intersects"].iloc[0], "Field should intersect protection zone"


# =============================================================================
# NULL Handling Tests
# =============================================================================


def test_null_identifier_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that NULL identifiers are handled correctly."""
    # Create data with NULL values
    pd.DataFrame(
        {
            "cvr": ["31373077", None, "12345678", None],
            "chr": ["123456", "654321", None, None],
            "company_name": ["Arla", "Unknown", "Test", "Invalid"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_data AS SELECT * FROM df")

    # Filter out NULL identifiers
    result = mock_duckdb_connection.execute(
        """
        SELECT *
        FROM test_data
        WHERE cvr IS NOT NULL OR chr IS NOT NULL
    """
    ).fetchdf()

    # Should have 3 rows (excluding last row with all NULLs)
    assert len(result) == 3, "Should have 3 rows with at least one valid identifier"

    # Check that we have valid identifiers
    assert len(result) > 0, "Should have at least one valid row"
    # At least some rows should have valid CVR or CHR
    assert result["cvr"].notna().sum() > 0 or result["chr"].notna().sum() > 0


def test_null_geometry_handling(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that NULL geometries are handled correctly."""
    pd.DataFrame(
        {
            "field_id": ["F001", "F002", "F003"],
            "geom_wkt": ["POINT(12.5 55.6)", None, "POINT(10.2 56.1)"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_fields AS SELECT * FROM df")

    # Convert to geometries, handling NULLs
    result = mock_duckdb_connection.execute(
        """
        SELECT
            field_id,
            CASE WHEN geom_wkt IS NULL THEN NULL
                 ELSE ST_GeomFromText(geom_wkt)
            END as geom,
            geom_wkt IS NOT NULL as has_geometry
        FROM test_fields
    """
    ).fetchdf()

    # Verify NULL handling
    assert result.loc[0, "has_geometry"]
    assert not result.loc[1, "has_geometry"]
    assert result.loc[2, "has_geometry"]


# =============================================================================
# Duplicate Detection Tests
# =============================================================================


def test_duplicate_detection_by_cvr(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that duplicate CVR records are detected."""
    # Create data with duplicates
    pd.DataFrame(
        {
            "cvr": ["31373077", "31373077", "12345678", "31373077"],
            "company_name": ["Arla Foods", "Arla Foods", "Test Co", "Arla Foods"],
            "record_date": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-02-01"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE companies AS SELECT * FROM df")

    # Detect duplicates
    result = mock_duckdb_connection.execute(
        """
        SELECT
            cvr,
            record_date,
            COUNT(*) as duplicate_count
        FROM companies
        GROUP BY cvr, record_date
        HAVING COUNT(*) > 1
    """
    ).fetchdf()

    # Should detect 2 duplicates for Arla on 2024-01-01
    assert len(result) == 1, "Should detect 1 duplicate group"
    assert result.loc[0, "cvr"] == "31373077"
    assert result.loc[0, "duplicate_count"] == 2


def test_duplicate_prevention_with_upsert(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that duplicates are prevented using upsert pattern."""
    # Initial data
    pd.DataFrame(
        {
            "cvr": ["31373077", "12345678"],
            "company_name": ["Arla Foods", "Test Co"],
            "updated_at": ["2024-01-01", "2024-01-01"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE companies AS SELECT * FROM df1")

    # New data with overlapping CVR (simulating upsert)
    pd.DataFrame(
        {
            "cvr": ["31373077", "87654321"],
            "company_name": ["Arla Foods Updated", "New Co"],
            "updated_at": ["2024-02-01", "2024-02-01"],
        }
    )

    # Simulate upsert: Delete existing, insert all
    mock_duckdb_connection.execute(
        "DELETE FROM companies WHERE cvr IN (SELECT cvr FROM df2)"
    )

    mock_duckdb_connection.execute("INSERT INTO companies SELECT * FROM df2")

    # Verify no duplicates
    result = mock_duckdb_connection.execute(
        """
        SELECT cvr, COUNT(*) as count
        FROM companies
        GROUP BY cvr
    """
    ).fetchdf()

    # All CVRs should be unique
    assert all(result["count"] == 1), "All CVRs should be unique after upsert"
    assert len(result) == 3, "Should have 3 unique companies"


# =============================================================================
# Data Type Consistency Tests
# =============================================================================


def test_data_type_consistency_identifiers(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that identifiers maintain string data type."""
    # Create data with proper types
    pd.DataFrame(
        {
            "cvr": ["31373077", "00113115", "12345678"],
            "chr": ["123456", "000123", "654321"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_identifiers AS SELECT * FROM df")

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
    pd.DataFrame(
        {
            "field_id": ["F001", "F002", "F003"],
            "area_ha": [10.5, 20.3, 15.7],
            "year": [2024, 2024, 2023],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_fields AS SELECT * FROM df")

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

    # Check numeric types
    assert area_type == "DOUBLE", f"area_ha should be DOUBLE, got {area_type}"
    assert year_type == "BIGINT", f"year should be BIGINT, got {year_type}"


def test_data_type_consistency_dates(
    mock_duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that date fields maintain proper data types."""
    # Create data with dates
    pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "record_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE test_records AS SELECT * FROM df")

    # Check data types
    result = mock_duckdb_connection.execute(
        """
        SELECT typeof(record_date) as date_type
        FROM test_records
        LIMIT 1
    """
    ).fetchone()

    date_type = result[0]

    # Should be TIMESTAMP, TIMESTAMP_NS, or DATE (depending on DuckDB/pandas version)
    assert date_type in ["TIMESTAMP", "TIMESTAMP_NS", "DATE"], (
        f"record_date should be timestamp-like, got {date_type}"
    )


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

    pd.DataFrame(
        {
            "cvr_raw": [31373077, "113115"],
            "company_name": ["Arla Foods", "Test Co"],
            "location_utm_wkt": [copenhagen["epsg_25832"], copenhagen["epsg_25832"]],
            "_fetch_timestamp": ["2024-01-01", "2024-01-01"],
            "_source": ["wfs_api", "wfs_api"],
        }
    )

    mock_duckdb_connection.execute(
        "CREATE TABLE bronze_companies AS SELECT * FROM bronze_df"
    )

    # Silver: Normalize and validate
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
    result = mock_duckdb_connection.execute(
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
    ).fetchdf()

    # Verify all quality checks pass
    assert all(result["cvr_length_valid"]), "All CVRs should be 8 digits"
    assert all(result["cvr_format_valid"]), "All CVRs should match format"

    # Check locations are in Denmark (accounting for axis order swap in ST_Transform)
    for _, row in result.iterrows():
        lon, lat = row["lon"], row["lat"]
        in_bounds = (
            (7.5 <= lon <= 15.5 and 54.5 <= lat <= 58.0)  # standard order
            or (7.5 <= lat <= 15.5 and 54.5 <= lon <= 58.0)  # swapped order
        )
        assert in_bounds, f"Location ({lon}, {lat}) should be in Denmark bounds"

    # Verify specific values
    assert "31373077" in result["cvr"].values, "Arla CVR should be normalized"
    assert "00113115" in result["cvr"].values, "Leading zeros should be preserved"
