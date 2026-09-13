"""Regression tests for NLES5 climate/field CRS handling."""

from types import SimpleNamespace

import duckdb

from unified_pipeline.gold.nles5_nitrogen_estimation.climate_processor import (
    NLES5ClimateProcessor,
)


class _Log:
    def debug(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        pass

    def info(self, *_args, **_kwargs):
        pass

    def warning(self, *_args, **_kwargs):
        pass


def test_year_climate_join_uses_processing_crs_for_both_geometries() -> None:
    """A WGS84 climate point must be compared to an EPSG:25832 field in metres."""
    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")

    connection.execute(
        """
        CREATE TABLE agricultural_fields_spatial AS
        SELECT
            'field-1'::VARCHAR AS field_id,
            'field-1'::VARCHAR AS field_uuid,
            '12345678'::VARCHAR AS cvr_number,
            2025::INTEGER AS year,
            ST_Transform(
                ST_Point(10.0, 56.0),
                'EPSG:4326',
                'EPSG:25832',
                always_xy := true
            ) AS geom,
            10.0::DOUBLE AS area_ha,
            'wheat'::VARCHAR AS crop_name,
            'arable'::VARCHAR AS layer_type,
            true AS grundbetaling_eligible
        """
    )
    connection.execute(
        """
        CREATE TABLE climate_percolation AS
        SELECT
            2025::INTEGER AS year,
            ST_Point(10.0, 56.0) AS geometry,
            100.0::DOUBLE AS perco_apr_aug_current,
            200.0::DOUBLE AS perco_sep_mar_current,
            90.0::DOUBLE AS perco_apr_aug_previous,
            190.0::DOUBLE AS perco_sep_mar_previous,
            300.0::DOUBLE AS total_percolation,
            500.0::DOUBLE AS avg_precipitation,
            200.0::DOUBLE AS avg_evaporation,
            true AS sufficient_climate_data
        """
    )

    processor = SimpleNamespace(
        config=SimpleNamespace(spatial_join_batch_size=1000),
        log=_Log(),
        conn=connection,
        storage_access=None,
    )

    result_table = NLES5ClimateProcessor(processor)._spatial_join_year_climate(
        2025, "climate_percolation"
    )
    assert result_table == "fields_climate_2025"

    row = connection.execute(
        f"""
        SELECT
            ST_X(climate_point),
            ST_Y(climate_point),
            distance_to_climate,
            total_percolation
        FROM {result_table}
        """
    ).fetchone()

    assert row is not None
    assert 400_000 < row[0] < 950_000
    assert 6_000_000 < row[1] < 6_500_000
    assert row[2] < 1.0
    assert row[3] == 300.0


def test_climate_processing_preserves_wgs84_points_outside_denmark_bounds() -> None:
    """Real DMI grid points east of 15°E must not take the scaling fallback."""
    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    connection.execute(
        """
        CREATE TABLE dmi_data AS
        SELECT
            'acc_precip'::VARCHAR AS parameter_id,
            '2024-04-01T00:00:00'::VARCHAR AS valid_time,
            '2024-04-02T00:00:00'::VARCHAR AS created,
            100.0::DOUBLE AS avg_value,
            100.0::DOUBLE AS min_value,
            100.0::DOUBLE AS max_value,
            1::BIGINT AS count,
            0.0::DOUBLE AS stddev_value,
            ST_AsGeoJSON(ST_Point(15.224529445129244, 55.29241404943446))::JSON
                AS centroid_geometry,
            'EPSG:25832'::VARCHAR AS source_crs,
            'EPSG:25832'::VARCHAR AS target_crs
        """
    )

    processor = SimpleNamespace(
        config=SimpleNamespace(),
        log=_Log(),
        conn=connection,
        storage_access=None,
    )

    result_table = NLES5ClimateProcessor(processor)._process_climate_data()
    row = connection.execute(
        f"SELECT ST_X(geometry), ST_Y(geometry) FROM {result_table}"
    ).fetchone()

    assert row is not None
    assert row[0] == 15.224529445129244
    assert row[1] == 55.29241404943446


def test_year_climate_join_skips_non_finite_projected_points() -> None:
    """One malformed projected point must not abort a year's real climate join."""
    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")

    connection.execute(
        """
        CREATE TABLE agricultural_fields_spatial AS
        SELECT
            'field-1'::VARCHAR AS field_id,
            'field-1'::VARCHAR AS field_uuid,
            '12345678'::VARCHAR AS cvr_number,
            2025::INTEGER AS year,
            ST_Transform(
                ST_Point(10.0, 56.0),
                'EPSG:4326',
                'EPSG:25832',
                always_xy := true
            ) AS geom,
            10.0::DOUBLE AS area_ha,
            'wheat'::VARCHAR AS crop_name,
            'arable'::VARCHAR AS layer_type,
            true AS grundbetaling_eligible
        """
    )
    connection.execute(
        """
        CREATE TABLE climate_percolation AS
        SELECT * FROM (VALUES
            (2025, ST_Point(10.0, 56.0), 100.0, 200.0, 90.0, 190.0, 300.0, 500.0, 200.0, true),
            (2025, ST_Point(86.0, 166.0), 1.0, 2.0, 1.0, 2.0, 3.0, 4.0, 5.0, true)
        ) AS climate(
            year, geometry, perco_apr_aug_current, perco_sep_mar_current,
            perco_apr_aug_previous, perco_sep_mar_previous, total_percolation,
            avg_precipitation, avg_evaporation, sufficient_climate_data
        )
        """
    )

    processor = SimpleNamespace(
        config=SimpleNamespace(spatial_join_batch_size=1000),
        log=_Log(),
        conn=connection,
        storage_access=None,
    )

    result_table = NLES5ClimateProcessor(processor)._spatial_join_year_climate(
        2025, "climate_percolation"
    )
    row = connection.execute(
        f"SELECT total_percolation, distance_to_climate FROM {result_table}"
    ).fetchone()

    assert row == (300.0, 0.0)
