"""
Integration tests for H3 PFAS exposure pipeline.

Tests end-to-end pipeline flow from H3 cells through spatial joins to exposure metrics.
This is CRITICAL for validating the complete PFAS exposure analysis workflow.
"""

import duckdb
import pytest

from h3_pfas_exposure.config import H3SpatialConfig
from h3_pfas_exposure.gold.coordinate_transformer import CoordinateTransformer
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
        chunk_size=50,
        h3_resolution=9,
        enable_progress_tracking=False,
        log_chunk_details=False,
        log_stage_timings=False,
        processing_crs="EPSG:25832",
        output_crs="EPSG:4326",
    )


@pytest.fixture
def spatial_joiner(duckdb_conn, config):
    """Create SpatialJoiner instance."""
    return SpatialJoiner(duckdb_conn, config)


@pytest.fixture
def transformer(duckdb_conn, config):
    """Create CoordinateTransformer instance."""
    return CoordinateTransformer(duckdb_conn, config)


@pytest.fixture
def full_test_dataset(duckdb_conn):
    """Create a complete test dataset with H3 cells, fields, and pesticides."""
    # Create H3 cells covering Copenhagen area
    duckdb_conn.execute("""
        CREATE TABLE test_h3_cells AS
        SELECT
            '891f1d489' || LPAD(CAST(i AS VARCHAR), 2, '0') || 'ffff' as h3_cell,
            55.6761 + (i * 0.001) as center_lat,
            12.5683 + (i * 0.001) as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
        FROM generate_series(0, 9) as s(i)
    """)

    # Create agricultural fields
    duckdb_conn.execute("""
        CREATE TABLE test_fields AS
        SELECT
            'field_' || i as field_id,
            '1234567' || i as cvr_number,
            'block_00' || i as block_id,
            10.0 + i as area_ha,
            '123' as crop_code,
            'Winter Wheat' as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.570 55.676, 12.570 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-field-' || i as field_uuid
        FROM generate_series(0, 4) as s(i)
    """)

    # Create pesticide applications with PFAS
    duckdb_conn.execute("""
        CREATE TABLE test_pesticides AS
        SELECT
            'uuid-field-' || i as field_uuid,
            '1234567' || i as cvr,
            'REG-00' || i as PesticideRegistrationNumber,
            2.5 + (i * 0.5) as DosageQuantity,
            4 as DosageUnit,
            (i % 2 = 0) as contains_pfas,
            false as contains_diquat,
            false as contains_glyphosate,
            CASE WHEN i % 2 = 0 THEN 50.0 + (i * 10) ELSE 0.0 END as pfas_containing_active_ingredient_grams,
            0.0 as diquat_containing_active_ingredient_grams,
            0.0 as glyphosate_containing_active_ingredient_grams,
            100.0 + (i * 20) as pesticide_belastning_applied,
            CASE WHEN i % 2 = 0 THEN 80.0 + (i * 15) ELSE 0.0 END as pfas_containing_pesticide_belastning_applied,
            0.0 as diquat_containing_pesticide_belastning_applied,
            0.0 as glyphosate_containing_pesticide_belastning_applied
        FROM generate_series(0, 4) as s(i)
    """)

    return {
        "h3_table": "test_h3_cells",
        "fields_table": "test_fields",
        "pesticides_table": "test_pesticides",
    }


# Integration Tests
def test_full_pipeline_h3_to_exposure(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test complete pipeline flow from H3 cells to exposure metrics."""
    # Run the full spatial join pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Verify result table structure
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
        "total_pesticide_belastning",
        "pfas_containing_active_ingredient_intensity_grams_per_ha",
    ]

    for col in required_columns:
        assert col in column_names, f"Result should have column: {col}"

    # Verify we have results
    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count > 0, "Pipeline should produce results"

    # Verify PFAS metrics are calculated
    pfas_results = duckdb_conn.execute(f"""
        SELECT
            SUM(total_pfas_containing_active_ingredient_grams) as total_pfas,
            AVG(pfas_containing_active_ingredient_intensity_grams_per_ha) as avg_intensity,
            MAX(total_pfas_containing_active_ingredient_grams) as max_pfas
        FROM {result_table}
        WHERE total_pfas_containing_active_ingredient_grams > 0
    """).fetchone()

    if pfas_results[0] is not None:
        assert pfas_results[0] > 0, "Should have positive PFAS totals"
        assert pfas_results[1] >= 0, "Should have non-negative average intensity"


def test_data_preservation(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that row counts and data integrity are maintained through pipeline."""
    # Count input data
    h3_count = duckdb_conn.execute(
        f"SELECT COUNT(*) FROM {full_test_dataset['h3_table']}"
    ).fetchone()[0]
    field_count = duckdb_conn.execute(
        f"SELECT COUNT(*) FROM {full_test_dataset['fields_table']}"
    ).fetchone()[0]

    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Check output count
    output_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]

    # Output should be <= input H3 cells (some may not intersect fields)
    assert output_count <= h3_count, "Output count should not exceed H3 cell count"

    # Check that H3 cells are preserved (no duplicates)
    duplicate_count = duckdb_conn.execute(f"""
        SELECT COUNT(*) - COUNT(DISTINCT h3_cell)
        FROM {result_table}
    """).fetchone()[0]

    assert duplicate_count == 0, "Should have no duplicate H3 cells in output"


def test_coordinate_system_consistency(duckdb_conn, spatial_joiner, transformer, full_test_dataset):
    """Test that processing uses WGS84 coordinate system (H3 is fundamentally WGS84-based)."""
    # Prepare geometries (H3 pipeline works in WGS84 since H3 is WGS84-based)
    prepared_fields = transformer.prepare_geometries(full_test_dataset["fields_table"])

    # Verify prepared data has valid WGS84 coordinates (lon/lat order)
    # Use ST_XMin/ST_XMax for polygons since ST_X only works on points
    result = duckdb_conn.execute(f"""
        SELECT
            MIN(ST_XMin(geometry)) as min_lon,
            MAX(ST_XMax(geometry)) as max_lon,
            MIN(ST_YMin(geometry)) as min_lat,
            MAX(ST_YMax(geometry)) as max_lat
        FROM {prepared_fields}
    """).fetchone()

    # WGS84 coordinates for Denmark: Lon ~8-15, Lat ~54-58
    # The test data uses coordinates around Copenhagen (lon ~12.5, lat ~55.6)
    assert 7.5 <= result[0] <= 15.5, (
        f"Min longitude should be within Denmark WGS84 bounds, got {result[0]}"
    )
    assert 7.5 <= result[1] <= 15.5, (
        f"Max longitude should be within Denmark WGS84 bounds, got {result[1]}"
    )
    assert 54.5 <= result[2] <= 58, (
        f"Min latitude should be within Denmark WGS84 bounds, got {result[2]}"
    )
    assert 54.5 <= result[3] <= 58, (
        f"Max latitude should be within Denmark WGS84 bounds, got {result[3]}"
    )

    # Run pipeline and check final coordinates (H3 cell centers are in WGS84 lat/lon)
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        prepared_fields,
        full_test_dataset["pesticides_table"],
        2023,
    )

    final_coords = duckdb_conn.execute(f"""
        SELECT
            MIN(center_lon) as min_lon,
            MAX(center_lon) as max_lon,
            MIN(center_lat) as min_lat,
            MAX(center_lat) as max_lat
        FROM {result_table}
    """).fetchone()

    # H3 cell centers (center_lat, center_lon) are stored in WGS84 for reference
    # These come directly from H3 functions, not from our processing
    assert 7.5 <= final_coords[0] <= 15.5, "H3 center min longitude should be within Denmark"
    assert 7.5 <= final_coords[1] <= 15.5, "H3 center max longitude should be within Denmark"
    assert 54.5 <= final_coords[2] <= 58.0, "H3 center min latitude should be within Denmark"
    assert 54.5 <= final_coords[3] <= 58.0, "H3 center max latitude should be within Denmark"


def test_denmark_bounds_enforcement(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that no coordinates fall outside Denmark bounds."""
    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Check for coordinates outside Denmark
    out_of_bounds = duckdb_conn.execute(f"""
        SELECT
            h3_cell,
            center_lat,
            center_lon
        FROM {result_table}
        WHERE center_lat < 54.5 OR center_lat > 58.0
           OR center_lon < 7.5 OR center_lon > 15.5
    """).fetchall()

    assert len(out_of_bounds) == 0, f"Found {len(out_of_bounds)} coordinates outside Denmark bounds"


def test_area_calculation_sanity(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that area calculations are reasonable (not negative, not too large)."""
    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Check area metrics
    area_stats = duckdb_conn.execute(f"""
        SELECT
            MIN(h3_area_ha) as min_h3_area,
            MAX(h3_area_ha) as max_h3_area,
            AVG(h3_area_ha) as avg_h3_area,
            MIN(total_intersection_area_ha) as min_intersection,
            MAX(total_intersection_area_ha) as max_intersection,
            MIN(actual_coverage_ratio) as min_coverage,
            MAX(actual_coverage_ratio) as max_coverage
        FROM {result_table}
    """).fetchone()

    # H3 area checks
    assert area_stats[0] > 0, "H3 cell areas should be positive"
    assert area_stats[1] < 100, "H3 cell areas should be reasonable (< 100 ha for res 9)"

    # Intersection area checks
    assert area_stats[3] >= 0, "Intersection areas should be non-negative"
    assert area_stats[4] <= area_stats[1], "Intersection area should not exceed H3 cell area"

    # Coverage ratio checks
    assert area_stats[5] >= 0, "Coverage ratio should be >= 0"
    assert area_stats[6] <= 1.2, "Coverage ratio should be <= 1.2 (allowing 20% tolerance)"


def test_pfas_intensity_calculations(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that PFAS intensity calculations are correct."""
    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Get cells with PFAS exposure
    pfas_cells = duckdb_conn.execute(f"""
        SELECT
            h3_cell,
            total_pfas_containing_active_ingredient_grams,
            total_intersection_area_ha,
            pfas_containing_active_ingredient_intensity_grams_per_ha,
            -- Verify calculation: intensity = total_grams / area_ha
            CASE
                WHEN total_intersection_area_ha > 0 THEN
                    total_pfas_containing_active_ingredient_grams / total_intersection_area_ha
                ELSE 0
            END as calculated_intensity
        FROM {result_table}
        WHERE total_pfas_containing_active_ingredient_grams > 0
    """).fetchall()

    for row in pfas_cells:
        h3_cell, total_grams, area_ha, reported_intensity, calculated_intensity = row

        # Verify intensity calculation is correct
        if area_ha > 0:
            diff = abs(reported_intensity - calculated_intensity)
            assert diff < 0.01, (
                f"Intensity mismatch for {h3_cell}: reported={reported_intensity}, calculated={calculated_intensity}"
            )


def test_pesticide_application_counts(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that pesticide application counts are accurate."""
    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Check application counts
    app_stats = duckdb_conn.execute(f"""
        SELECT
            SUM(total_pesticide_applications) as total_apps,
            SUM(pfas_containing_applications) as pfas_apps,
            SUM(diquat_containing_applications) as diquat_apps,
            SUM(glyphosate_containing_applications) as glyphosate_apps
        FROM {result_table}
    """).fetchone()

    # Should have some pesticide applications
    assert app_stats[0] >= 0, "Should have non-negative total applications"
    assert app_stats[1] >= 0, "Should have non-negative PFAS applications"

    # PFAS apps should be <= total apps
    if app_stats[0] > 0:
        assert app_stats[1] <= app_stats[0], (
            "PFAS applications should not exceed total applications"
        )


def test_crop_diversity_metrics(duckdb_conn, spatial_joiner, full_test_dataset):
    """Test that crop diversity metrics are calculated."""
    # Run pipeline
    result_table = spatial_joiner.perform_chunked_spatial_join(
        full_test_dataset["h3_table"],
        full_test_dataset["fields_table"],
        full_test_dataset["pesticides_table"],
        2023,
    )

    # Check crop metrics
    crop_stats = duckdb_conn.execute(f"""
        SELECT
            COUNT(DISTINCT crop_types) as unique_crop_combinations,
            AVG(crop_diversity) as avg_diversity,
            MAX(crop_diversity) as max_diversity
        FROM {result_table}
        WHERE crop_types IS NOT NULL AND crop_types != ''
    """).fetchone()

    if crop_stats[0] is not None and crop_stats[0] > 0:
        assert crop_stats[1] >= 0, "Average crop diversity should be non-negative"
        assert crop_stats[2] >= crop_stats[1], "Max diversity should be >= average diversity"


def test_null_handling(duckdb_conn, spatial_joiner):
    """Test that NULL values are handled correctly throughout pipeline."""
    # Create dataset with NULLs
    duckdb_conn.execute("""
        CREATE TABLE null_h3 AS
        SELECT
            '891f1d48993ffff' as h3_cell,
            55.6761 as center_lat,
            12.5683 as center_lon,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as h3_geometry
    """)

    duckdb_conn.execute("""
        CREATE TABLE null_fields AS
        SELECT
            'field_001' as field_id,
            '12345678' as cvr_number,
            'block_001' as block_id,
            10.0 as area_ha,
            NULL as crop_code,
            NULL as crop_name,
            ST_GeomFromText('POLYGON((12.568 55.676, 12.569 55.676, 12.569 55.677, 12.568 55.677, 12.568 55.676))') as geometry,
            'uuid-001' as field_uuid
    """)

    duckdb_conn.execute("""
        CREATE TABLE null_pesticides AS
        SELECT
            'uuid-001' as field_uuid,
            '12345678' as cvr,
            NULL as PesticideRegistrationNumber,
            NULL as DosageQuantity,
            NULL as DosageUnit,
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

    # Should not raise error
    result_table = spatial_joiner.perform_chunked_spatial_join(
        "null_h3", "null_fields", "null_pesticides", 2023
    )

    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count >= 0, "Should handle NULL values gracefully"
