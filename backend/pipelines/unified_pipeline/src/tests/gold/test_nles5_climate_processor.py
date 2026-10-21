"""Regression tests for NLES5 climate/field CRS handling."""

from types import SimpleNamespace

import duckdb
import pytest

from unified_pipeline.gold.nles5_nitrogen_estimation.climate_processor import (
    NLES5ClimateProcessor,
)
from unified_pipeline.gold.nles5_nitrogen_estimation.data_loader import NLES5DataLoader


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


def test_dmi_combination_feeds_evaporation_into_net_percolation() -> None:
    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")

    connection.execute(
        """
        CREATE TABLE dmi_precipitation AS
        SELECT
            'acc_precip'::VARCHAR AS parameter_id,
            '2025-04-01'::TIMESTAMP AS valid_time,
            40.0::DOUBLE AS avg_value,
            '{"type":"Point","coordinates":[10.0,56.0]}'::VARCHAR
                AS centroid_geometry,
            'EPSG:4326'::VARCHAR AS source_crs,
            'EPSG:4326'::VARCHAR AS target_crs
        """
    )
    connection.execute(
        """
        CREATE TABLE dmi_evaporation AS
        SELECT
            'pot_evaporation_makkink'::VARCHAR AS parameter_id,
            '2025-04-01'::TIMESTAMP AS valid_time,
            25.0::DOUBLE AS avg_value,
            '{"type":"Point","coordinates":[10.0,56.0]}'::VARCHAR
                AS centroid_geometry,
            'EPSG:4326'::VARCHAR AS source_crs,
            'EPSG:4326'::VARCHAR AS target_crs
        """
    )

    processor = SimpleNamespace(
        config=SimpleNamespace(bucket="landbruget-data"),
        log=_Log(),
        conn=connection,
        storage_access=None,
    )
    NLES5DataLoader(processor)._combine_dmi_datasets()

    result_table = NLES5ClimateProcessor(processor)._process_climate_data()
    row = connection.execute(
        f"""
        SELECT perco_apr_aug_current, total_percolation,
               avg_precipitation, avg_evaporation
        FROM {result_table}
        WHERE year = 2025
        """
    ).fetchone()

    assert row == pytest.approx((15.0, 15.0, 40.0, 25.0))
