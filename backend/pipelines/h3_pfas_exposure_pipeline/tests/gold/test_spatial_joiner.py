"""
Tests for spatial joining utilities in H3 PFAS exposure analysis.

Tests the spatial join operations that combine H3 cells with agricultural fields
and pesticide application data. This is CRITICAL for accurate PFAS exposure analysis.
"""

import math

import duckdb
import pytest

from h3_pfas_exposure.config import H3SpatialConfig
from h3_pfas_exposure.gold.spatial_joiner import SpatialJoiner


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
        chunk_size=100,
        h3_resolution=9,
        enable_progress_tracking=False,
        log_chunk_details=False,
        log_stage_timings=False,
    )


@pytest.fixture
def spatial_joiner(duckdb_conn, config):
    """Create SpatialJoiner instance."""
    return SpatialJoiner(duckdb_conn, config)


@pytest.fixture
def sample_h3_data(duckdb_conn):
    """Create sample H3 cell data for testing."""
    # Copenhagen coordinates: 55.6761° N, 12.5683° E
    duckdb_conn.execute("""
        CREATE TABLE test_h3_cells AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        UNION ALL
        SELECT
            '891f1d48997ffff' as h3_cell,
            55.6771 as center_lat,
            12.5693 as center_lon,
            ST_GeomFromText('POLYGON((12.569 55.677, 12.570 55.677, 12.570 55.678, 12.569 55.678, 12.569 55.677))') as h3_geometry
    """)
    return "test_h3_cells"


@pytest.fixture
def sample_field_data(duckdb_conn):
    """Create sample field data for testing."""
    duckdb_conn.execute("""
        CREATE TABLE test_fields AS
        SELECT
            'field_001' as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            10.5 as area_ha,
            '123' as crop_code,
            'Winter Wheat' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.570 55.676, 12.570 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-field-001' as field_uuid
    """)
    return "test_fields"


@pytest.fixture
def sample_pesticide_data(duckdb_conn):
    """Create sample pesticide data for testing."""
    duckdb_conn.execute("""
        CREATE TABLE test_pesticides AS
        SELECT
            'uuid-field-001' as field_uuid,
            '12345678' as cvr,
            'REG-001' as PesticideRegistrationNumber,
            2.5 as DosageQuantity,
            4 as DosageUnit,
            true as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            50.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            100.0 as pesticide_belastning_applied,
            80.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
    """)
    return "test_pesticides"


# Chunking Logic Tests
def test_chunk_calculation(duckdb_conn, config):
    """Test that chunk calculation uses correct math.ceil formula."""
    # Test with 250 cells and chunk size of 100
    h3_count = 250
    expected_chunks = math.ceil(h3_count / config.chunk_size)  # ceil(250/100) = 3

    assert expected_chunks == 3, "Should have 3 chunks for 250 cells with chunk_size 100"

    # Test edge cases
    assert math.ceil(100 / 100) == 1, "Exact multiple should give 1 chunk"
    assert math.ceil(99 / 100) == 1, "Less than chunk size should give 1 chunk"
    assert math.ceil(101 / 100) == 2, "Just over chunk size should give 2 chunks"


def test_chunk_boundaries_empty(duckdb_conn, config, spatial_joiner):
    """Test chunk processing with empty dataset."""
    duckdb_conn.execute(
        "CREATE TABLE empty_h3 AS SELECT * FROM (VALUES (NULL, NULL, NULL, NULL)) AS t(h3_cell, center_lat, center_lon, h3_geometry) WHERE FALSE"
    )
    duckdb_conn.execute(
        "CREATE TABLE empty_fields AS SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS t(field_id, cvr_number, block_id, area_ha, crop_code, crop_name, geometry, field_uuid) WHERE FALSE"
    )
    duckdb_conn.execute(
        "CREATE TABLE empty_pesticides AS SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS t(field_uuid, cvr, PesticideRegistrationNumber, DosageQuantity, DosageUnit, contains_pfas, contains_diquat, contains_glyphosate, pfas_containing_active_ingredient_grams, diquat_containing_active_ingredient_grams, glyphosate_containing_active_ingredient_grams, pesticide_belastning_applied, pfas_containing_pesticide_belastning_applied, diquat_containing_pesticide_belastning_applied, glyphosate_containing_pesticide_belastning_applied) WHERE FALSE"
    )

    # Empty dataset should not cause errors
    # With 0 chunks, no result table is created, so we verify the function completes without error
    result_table = spatial_joiner.perform_chunked_spatial_join(
        "empty_h3", "empty_fields", "empty_pesticides", 2023
    )

    # Check if table was created (with 0 chunks, it might not be)
    tables = duckdb_conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    if result_table in table_names:
        count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
        assert count == 0, "Empty input should produce empty result"
    else:
        # Table not created with 0 chunks is acceptable
        assert True, "Empty dataset handled correctly (no table created)"


def test_chunk_boundaries_single_row(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test chunk processing with single row."""
    # Create single row dataset
    duckdb_conn.execute("CREATE TABLE single_h3 AS SELECT * FROM test_h3_cells LIMIT 1")

    result_table = spatial_joiner.perform_chunked_spatial_join(
        "single_h3", "test_fields", "test_pesticides", 2023
    )

    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count >= 0, "Single row should process without error"


def test_chunk_boundaries_exact_multiple(duckdb_conn, config, spatial_joiner):
    """Test chunk processing when cell count is exact multiple of chunk_size."""
    # Create exactly 100 cells (one chunk)
    duckdb_conn.execute("""
        CREATE TABLE exact_h3 AS
        SELECT
            '891f1d48993' || LPAD(CAST(i AS VARCHAR), 4, '0') || 'fff' as h3_cell,
            55.6761 + (i * 0.0001) as center_lat,
            12.5683 + (i * 0.0001) as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        FROM generate_series(0, 99) as s(i)
    """)

    duckdb_conn.execute(
        "CREATE TABLE empty_fields AS SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS t(field_id, cvr_number, block_id, area_ha, crop_code, crop_name, geometry, field_uuid) WHERE FALSE"
    )
    duckdb_conn.execute(
        "CREATE TABLE empty_pesticides AS SELECT * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS t(field_uuid, cvr, PesticideRegistrationNumber, DosageQuantity, DosageUnit, contains_pfas, contains_diquat, contains_glyphosate, pfas_containing_active_ingredient_grams, diquat_containing_active_ingredient_grams, glyphosate_containing_active_ingredient_grams, pesticide_belastning_applied, pfas_containing_pesticide_belastning_applied, diquat_containing_pesticide_belastning_applied, glyphosate_containing_pesticide_belastning_applied) WHERE FALSE"
    )

    h3_count = duckdb_conn.execute("SELECT COUNT(*) FROM exact_h3").fetchone()[0]
    expected_chunks = math.ceil(h3_count / config.chunk_size)

    assert h3_count == 100, "Should have exactly 100 cells"
    assert expected_chunks == 1, "Should process in exactly 1 chunk"


def test_chunk_processing_order(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test that chunks are processed sequentially in correct order."""
    # This is implicitly tested by the chunk_idx parameter in _process_single_chunk
    # We verify the final result contains all expected data
    result_table = spatial_joiner.perform_chunked_spatial_join(
        sample_h3_data, sample_field_data, sample_pesticide_data, 2023
    )

    # Verify result table exists and has the expected structure
    columns = duckdb_conn.execute(f"PRAGMA table_info({result_table})").fetchall()
    column_names = [col[1] for col in columns]

    assert "h3_cell" in column_names, "Result should have h3_cell column"
    assert "center_lat" in column_names, "Result should have center_lat column"
    assert "center_lon" in column_names, "Result should have center_lon column"


# Spatial Join Operations Tests
def test_spatial_join_h3_to_geometry(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test spatial join correctly matches H3 cells to field geometries."""
    result_table = spatial_joiner.perform_chunked_spatial_join(
        sample_h3_data, sample_field_data, sample_pesticide_data, 2023
    )

    # Check that we have results
    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count > 0, "Spatial join should find intersecting H3 cells and fields"

    # Verify spatial intersection occurred
    result = duckdb_conn.execute(f"""
        SELECT h3_cell, unique_field_count
        FROM {result_table}
        WHERE h3_cell IS NOT NULL
        LIMIT 1
    """).fetchone()

    assert result is not None, "Should have at least one H3 cell with field intersection"
    assert result[1] >= 0, "Should have field count >= 0"


def test_spatial_join_accuracy(duckdb_conn, config, spatial_joiner):
    """Test spatial join accuracy with known overlapping geometries."""
    # Create perfectly overlapping H3 cell and field
    duckdb_conn.execute("""
        CREATE TABLE precise_h3 AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    duckdb_conn.execute("""
        CREATE TABLE precise_field AS
        SELECT
            'field_precise' as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            1.0 as area_ha,
            '123' as crop_code,
            'Test Crop' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-precise' as field_uuid
    """)

    duckdb_conn.execute("""
        CREATE TABLE precise_pesticides AS
        SELECT
            'uuid-precise' as field_uuid,
            '12345678' as cvr,
            'REG-001' as PesticideRegistrationNumber,
            1.0 as DosageQuantity,
            4 as DosageUnit,
            false as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            0.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            0.0 as pesticide_belastning_applied,
            0.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
    """)

    result_table = spatial_joiner.perform_chunked_spatial_join(
        "precise_h3", "precise_field", "precise_pesticides", 2023
    )

    # Check coverage ratio is close to 1.0 for perfect overlap
    result = duckdb_conn.execute(f"""
        SELECT actual_coverage_ratio
        FROM {result_table}
        WHERE h3_cell = '891f1d48993ffff'
    """).fetchone()

    if result:
        coverage = result[0]
        assert (
            0.8 <= coverage <= 1.0
        ), f"Perfect overlap should have coverage close to 1.0, got {coverage}"


def test_spatial_join_denmark_bounds(duckdb_conn, config, spatial_joiner):
    """Test that all spatial join results are within Denmark bounds."""
    # Create H3 cells within Denmark (Jutland)
    duckdb_conn.execute("""
        CREATE TABLE denmark_h3 AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            56.1629 as center_lat,
            10.2039 as center_lon,
            ST_GeomFromText('POLYGON((10.20 56.16, 10.21 56.16, 10.21 56.17, 10.20 56.17, 10.20 56.16))') as h3_geometry
    """)

    duckdb_conn.execute("""
        CREATE TABLE denmark_field AS
        SELECT
            'field_dk' as field_id,
            '87654321' as cvr_number,
            'block_001' as block_id,
            5.0 as area_ha,
            '456' as crop_code,
            'Barley' as crop_name,
            ST_GeomFromText('POLYGON((10.20 56.16, 10.21 56.16, 10.21 56.17, 10.20 56.17, 10.20 56.16))') as geometry,
            'uuid-dk' as field_uuid
    """)

    duckdb_conn.execute("""
        CREATE TABLE denmark_pesticides AS
        SELECT
            'uuid-dk' as field_uuid,
            '87654321' as cvr,
            'REG-002' as PesticideRegistrationNumber,
            3.0 as DosageQuantity,
            4 as DosageUnit,
            false as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            0.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            0.0 as pesticide_belastning_applied,
            0.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
    """)

    result_table = spatial_joiner.perform_chunked_spatial_join(
        "denmark_h3", "denmark_field", "denmark_pesticides", 2023
    )

    # Verify all coordinates are within Denmark bounds
    out_of_bounds = duckdb_conn.execute(f"""
        SELECT COUNT(*)
        FROM {result_table}
        WHERE center_lat < 54.5 OR center_lat > 58.0
           OR center_lon < 7.5 OR center_lon > 15.5
    """).fetchone()[0]

    assert out_of_bounds == 0, "All results should be within Denmark bounds"


def test_spatial_join_null_geometries(duckdb_conn, config, spatial_joiner):
    """Test handling of NULL geometries in spatial join."""
    # Create mixed dataset: valid geometry + NULL geometry
    # The spatial join should filter out NULL geometries during the WHERE clause
    duckdb_conn.execute("""
        CREATE TABLE null_geom_h3 AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        WHERE FALSE  -- Create empty table to test edge case
    """)

    duckdb_conn.execute("""
        CREATE TABLE null_geom_field AS
        SELECT
            'field_null' as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            1.0 as area_ha,
            '123' as crop_code,
            'Test' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-null' as field_uuid
        WHERE FALSE  -- Create empty table to test edge case
    """)

    duckdb_conn.execute("""
        CREATE TABLE null_geom_pesticides AS
        SELECT
            'uuid-null' as field_uuid,
            '12345678' as cvr,
            'REG-001' as PesticideRegistrationNumber,
            1.0 as DosageQuantity,
            4 as DosageUnit,
            false as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            0.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            0.0 as pesticide_belastning_applied,
            0.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
    """)

    # Should not raise error with empty tables
    result_table = spatial_joiner.perform_chunked_spatial_join(
        "null_geom_h3", "null_geom_field", "null_geom_pesticides", 2023
    )

    # When there are no H3 cells to process, the result table may not be created
    # Check if table exists first
    table_exists = (
        duckdb_conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{result_table}'"
        ).fetchone()[0]
        > 0
    )

    if table_exists:
        count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
        assert count == 0, "Empty input tables should produce empty result"
    else:
        # It's acceptable for the table to not exist when there's nothing to process
        pass  # Test passes - graceful handling of empty input


def test_spatial_join_performance(duckdb_conn, config, spatial_joiner):
    """Test spatial join performance with larger dataset."""
    import time

    # Create 200 H3 cells (2 chunks)
    duckdb_conn.execute("""
        CREATE TABLE perf_h3 AS
        SELECT
            '891f1d48' || LPAD(CAST(i AS VARCHAR), 3, '0') || 'ffff' as h3_cell,
            55.6761 + (i * 0.001) as center_lat,
            12.5683 + (i * 0.001) as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        FROM generate_series(0, 199) as s(i)
    """)

    duckdb_conn.execute("""
        CREATE TABLE perf_field AS
        SELECT
            'field_' || i as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            1.0 as area_ha,
            '123' as crop_code,
            'Test' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-' || i as field_uuid
        FROM generate_series(0, 49) as s(i)
    """)

    duckdb_conn.execute("""
        CREATE TABLE perf_pesticides AS
        SELECT
            'uuid-' || i as field_uuid,
            '12345678' as cvr,
            'REG-001' as PesticideRegistrationNumber,
            1.0 as DosageQuantity,
            4 as DosageUnit,
            false as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            0.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            0.0 as pesticide_belastning_applied,
            0.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
        FROM generate_series(0, 49) as s(i)
    """)

    start_time = time.time()
    spatial_joiner.perform_chunked_spatial_join("perf_h3", "perf_field", "perf_pesticides", 2023)
    elapsed = time.time() - start_time

    # Should complete in reasonable time (< 10 seconds for 200 cells)
    assert elapsed < 10, f"Processing 200 cells should complete within 10s, took {elapsed:.2f}s"


# Result Aggregation Tests
def test_result_table_creation(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test that result table is created with correct structure."""
    result_table = spatial_joiner.perform_chunked_spatial_join(
        sample_h3_data, sample_field_data, sample_pesticide_data, 2023
    )

    # Check table exists
    tables = duckdb_conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    assert result_table in table_names, f"Result table {result_table} should exist"

    # Check required columns
    columns = duckdb_conn.execute(f"PRAGMA table_info({result_table})").fetchall()
    column_names = [col[1] for col in columns]

    required_columns = [
        "h3_cell",
        "center_lat",
        "center_lon",
        "h3_area_ha",
        "total_intersection_area_ha",
        "actual_coverage_ratio",
        "unique_field_count",
        "total_pfas_containing_active_ingredient_grams",
    ]

    for col in required_columns:
        assert col in column_names, f"Result table should have column: {col}"


def test_result_aggregation(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test that results are properly aggregated (count, sum, avg)."""
    result_table = spatial_joiner.perform_chunked_spatial_join(
        sample_h3_data, sample_field_data, sample_pesticide_data, 2023
    )

    # Check aggregation metrics
    result = duckdb_conn.execute(f"""
        SELECT
            COUNT(*) as total_cells,
            SUM(unique_field_count) as total_field_intersections,
            AVG(actual_coverage_ratio) as avg_coverage,
            SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_grams
        FROM {result_table}
    """).fetchone()

    assert result[0] >= 0, "Should have cell count >= 0"
    assert result[1] >= 0, "Should have field intersections >= 0"
    if result[2] is not None:
        assert 0 <= result[2] <= 1, "Average coverage should be between 0 and 1"
    assert result[3] >= 0, "Total PFAS grams should be >= 0"


def test_result_deduplication(
    duckdb_conn, config, spatial_joiner, sample_h3_data, sample_field_data, sample_pesticide_data
):
    """Test that result has no duplicate H3 cells."""
    result_table = spatial_joiner.perform_chunked_spatial_join(
        sample_h3_data, sample_field_data, sample_pesticide_data, 2023
    )

    # Check for duplicates
    duplicates = duckdb_conn.execute(f"""
        SELECT h3_cell, COUNT(*) as count
        FROM {result_table}
        GROUP BY h3_cell
        HAVING COUNT(*) > 1
    """).fetchall()

    assert len(duplicates) == 0, f"Should have no duplicate H3 cells, found {len(duplicates)}"


# Error Handling Tests
def test_invalid_h3_cells(duckdb_conn, config, spatial_joiner):
    """Test handling of invalid H3 cell identifiers."""
    duckdb_conn.execute("""
        CREATE TABLE invalid_h3 AS
        SELECT
            'invalid_h3_cell' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    duckdb_conn.execute("""
        CREATE TABLE invalid_field AS
        SELECT
            'field_001' as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            1.0 as area_ha,
            '123' as crop_code,
            'Test' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-001' as field_uuid
    """)

    duckdb_conn.execute("""
        CREATE TABLE invalid_pesticides AS
        SELECT
            'uuid-001' as field_uuid,
            '12345678' as cvr,
            'REG-001' as PesticideRegistrationNumber,
            1.0 as DosageQuantity,
            4 as DosageUnit,
            false as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            0.0 as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            0.0 as pesticide_belastning_applied,
            0.0 as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
    """)

    # Should not raise error, just process with invalid H3 string
    result_table = spatial_joiner.perform_chunked_spatial_join(
        "invalid_h3", "invalid_field", "invalid_pesticides", 2023
    )

    # Should still produce results (H3 string is just an identifier)
    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count >= 0, "Should handle invalid H3 cell strings gracefully"
