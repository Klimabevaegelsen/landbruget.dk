"""Tests for child receptor pesticide exposure gold calculations."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import duckdb
import pytest

from unified_pipeline.gold.child_receptor_exposure import (
    ChildReceptorExposureGold,
    ChildReceptorExposureGoldConfig,
    build_exposure_summary,
    compute_child_receptor_exposure,
)
from unified_pipeline.gold.pesticide_drift_exposure import rautmann_drift_pct


@pytest.fixture
def spatial_connection():
    conn = duckdb.connect(database=":memory:")
    conn.execute("LOAD spatial")
    yield conn
    conn.close()


def _create_empty_pesticide_tables(conn):
    conn.execute("""
        CREATE TABLE disaggregation (
            field_uuid VARCHAR,
            DosageQuantity DOUBLE,
            AllocatedArea DOUBLE,
            PesticideName VARCHAR
        )
    """)
    conn.execute("CREATE TABLE fields (field_uuid VARCHAR, geometry GEOMETRY)")


def _create_synthetic_exposure_data(conn):
    base_easting = 600000.0
    base_northing = 6200000.0
    conn.execute("""
        CREATE TABLE receptors (
            receptor_id VARCHAR,
            receptor_type VARCHAR,
            subtype VARCHAR,
            name VARCHAR,
            ownership VARCHAR,
            kommune_kode VARCHAR,
            address VARCHAR,
            cvr VARCHAR,
            source VARCHAR,
            matched_sources VARCHAR[],
            utm_e DOUBLE,
            utm_n DOUBLE,
            lon DOUBLE,
            lat DOUBLE,
            geometry GEOMETRY
        )
    """)
    receptors = [
        (
            "R1",
            "daycare",
            "vuggestue",
            "North Daycare",
            "public",
            "0101",
            "A Road",
            "12345678",
            "test",
            ["a", "b"],
            0,
        ),
        (
            "R2",
            "school",
            "primary",
            "Large School",
            "public",
            "0102",
            "B Road",
            "22345678",
            "test",
            ["a"],
            3000,
        ),
        (
            "R3",
            "playground",
            "public",
            "Park",
            "public",
            "0101",
            "C Road",
            None,
            "test",
            ["a"],
            6000,
        ),
        (
            "R4",
            "daycare",
            "private",
            "Remote Daycare",
            "private",
            "0103",
            "D Road",
            None,
            "test",
            ["b"],
            10000,
        ),
    ]
    for receptor in receptors:
        conn.execute(
            """INSERT INTO receptors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               ST_Point(?, ?))""",
            (
                *receptor[:10],
                base_easting + receptor[10],
                base_northing,
                9.0,
                56.0,
                base_easting + receptor[10],
                base_northing,
            ),
        )

    conn.execute("CREATE TABLE cadastral (bfe_number BIGINT, geometry GEOMETRY)")
    conn.execute(
        "INSERT INTO cadastral VALUES (1, ST_MakeEnvelope(?, ?, ?, ?))",
        (base_easting - 20, base_northing - 20, base_easting + 20, base_northing + 20),
    )
    conn.execute(
        "INSERT INTO cadastral VALUES (2, ST_MakeEnvelope(?, ?, ?, ?))",
        (base_easting + 2800, base_northing - 250, base_easting + 3200, base_northing + 250),
    )
    conn.execute(
        "INSERT INTO cadastral VALUES (3, ST_MakeEnvelope(?, ?, ?, ?))",
        (base_easting + 5980, base_northing - 20, base_easting + 6020, base_northing + 20),
    )

    conn.execute("""
        CREATE TABLE fields (field_uuid VARCHAR, geometry GEOMETRY)
    """)
    conn.execute(
        """
        INSERT INTO fields VALUES
            ('A', ST_MakeEnvelope(?, ?, ?, ?)),
            ('B', ST_MakeEnvelope(?, ?, ?, ?)),
            ('C', ST_MakeEnvelope(?, ?, ?, ?))
    """,
        (
            base_easting + 40,
            base_northing - 50,
            base_easting + 140,
            base_northing + 50,
            base_easting + 320,
            base_northing - 50,
            base_easting + 420,
            base_northing + 50,
            base_easting + 620,
            base_northing - 50,
            base_easting + 720,
            base_northing + 50,
        ),
    )
    conn.execute("""
        CREATE TABLE disaggregation (
            field_uuid VARCHAR,
            DosageQuantity DOUBLE,
            AllocatedArea DOUBLE,
            PesticideName VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO disaggregation VALUES
            ('A', 1.0, 5.0, 'P1'),
            ('A', 2.0, 5.0, 'P2'),
            ('B', 2.0, 10.0, 'P1'),
            ('C', 1.0, 4.0, 'P3')
    """)


def test_site_selection_metrics_and_summary(spatial_connection):
    conn = spatial_connection
    _create_synthetic_exposure_data(conn)

    count = compute_child_receptor_exposure(
        conn,
        "receptors",
        "cadastral",
        "disaggregation",
        "fields",
        pesticide_year=2020,
        enable_wind_weighting=False,
        batch_size=2,
    )

    assert count == 4
    rows = {
        row[0]: row
        for row in conn.execute("""
            SELECT receptor_id, site_geom_source, site_bfe_number, site_area_m2,
                   nearest_sprayed_field_m, sprayed_fields_within_10m,
                   sprayed_fields_within_30m, sprayed_fields_within_50m,
                   sprayed_fields_within_250m, sprayed_fields_within_500m,
                   sprayed_area_ha_within_250m, sprayed_area_ha_within_500m,
                   applied_kg_on_fields_within_500m,
                   unique_pesticides_within_500m, drift_dose_kg,
                   max_single_drift_pct, wind_weighted, top_pesticides,
                   drift_dose_percentile, pesticide_year, field_year
            FROM child_receptor_exposure_final
        """).fetchall()
    }

    daycare = rows["R1"]
    assert daycare[1:4] == ("cadastral", 1, 1600.0)
    assert daycare[4] == pytest.approx(20.0)
    assert daycare[5:10] == (0, 1, 1, 1, 2)
    assert daycare[10] == pytest.approx(1.0)
    assert daycare[11] == pytest.approx(2.0)
    assert daycare[12] == pytest.approx(35.0)
    assert daycare[13] == 2
    expected_dose = (
        15.0 * rautmann_drift_pct(20.0) / 100.0 + 20.0 * rautmann_drift_pct(300.0) / 100.0
    )
    assert daycare[14] == pytest.approx(expected_dose)
    assert daycare[15] == pytest.approx(rautmann_drift_pct(20.0))
    assert daycare[16] is False
    top_pesticides = json.loads(daycare[17])
    assert [entry["name"] for entry in top_pesticides] == ["P2", "P1"]
    assert daycare[19:21] == (2020, 2021)

    assert rows["R2"][1] == "point_large_parcel"
    assert rows["R2"][2] is None
    assert rows["R2"][3] is None
    assert rows["R3"][1] == "point"
    assert rows["R3"][2] is None

    remote = rows["R4"]
    assert remote[1] == "point"
    assert remote[4] is None
    assert remote[5:10] == (0, 0, 0, 0, 0)
    assert remote[10:18] == (0.0, 0.0, 0.0, 0, 0.0, None, False, "[]")

    summary = build_exposure_summary(
        conn, "child_receptor_exposure_final", 2020, [10, 30, 50, 250, 500]
    )
    daycare_summary = summary["by_receptor_type"]["daycare"]
    assert daycare_summary["total"] == 2
    assert daycare_summary["within_distance_bands"]["30m"] == {
        "count": 1,
        "share": 0.5,
    }
    assert summary["by_kommune_kode"]["0101"]["daycare"]["total"] == 1
    assert summary["by_kommune_kode"]["0102"]["school"]["total"] == 1


def test_cadastral_wgs84_is_transformed_to_processing_crs(spatial_connection):
    conn = spatial_connection
    _create_empty_pesticide_tables(conn)
    conn.execute("""
        CREATE TABLE receptors AS
        SELECT 'R1' AS receptor_id, 'daycare' AS receptor_type,
               NULL::VARCHAR AS subtype, 'Daycare' AS name,
               NULL::VARCHAR AS ownership, '0101' AS kommune_kode,
               NULL::VARCHAR AS address, NULL::VARCHAR AS cvr,
               'test' AS source, ['test']::VARCHAR[] AS matched_sources,
               600000.0 AS utm_e, 6200000.0 AS utm_n,
               9.0 AS lon, 56.0 AS lat,
               ST_Point(600000.0, 6200000.0) AS geometry
    """)
    conn.execute("""
        CREATE TABLE cadastral AS
        SELECT 42::BIGINT AS bfe_number,
               ST_Transform(
                   ST_MakeEnvelope(599980.0, 6199980.0, 600020.0, 6200020.0),
                   'EPSG:25832', 'EPSG:4326', always_xy := true
               ) AS geometry
    """)

    compute_child_receptor_exposure(
        conn,
        "receptors",
        "cadastral",
        "disaggregation",
        "fields",
        pesticide_year=2020,
        enable_wind_weighting=False,
    )

    result = conn.execute(
        "SELECT site_geom_source, site_bfe_number, site_area_m2 FROM child_receptor_exposure_final"
    ).fetchone()
    assert result == ("cadastral", 42, pytest.approx(1600.0))


@pytest.mark.asyncio
async def test_missing_child_receptors_logs_warning_and_saves_nothing():
    processor = object.__new__(ChildReceptorExposureGold)
    processor.config = ChildReceptorExposureGoldConfig(pesticide_year=2020)
    processor.log = Mock()
    processor.storage = Mock()
    processor._read_silver_data = Mock(return_value=None)
    processor._setup_duckdb = AsyncMock()
    processor._load_wind_data = Mock()
    processor._save_year_results = AsyncMock()

    await processor.run()

    processor._read_silver_data.assert_called_once_with("child_receptors")
    processor.storage.list_files.assert_not_called()
    processor._save_year_results.assert_not_awaited()
    assert processor.log.warning.call_count >= 1


@pytest.mark.asyncio
async def test_matrix_year_without_disaggregation_data_is_skipped():
    processor = object.__new__(ChildReceptorExposureGold)
    processor.config = ChildReceptorExposureGoldConfig(pesticide_year=2014)
    processor.log = Mock()
    processor._setup_duckdb = AsyncMock()
    processor._load_wind_data = Mock()
    processor._load_datasets = AsyncMock(
        return_value={
            "disaggregation": {2015: "path.parquet"},
            "receptors": "receptors",
            "cadastral": "cadastral",
        }
    )
    processor._load_year_data = AsyncMock()
    processor._compute_child_receptor_exposure = AsyncMock()
    processor._save_year_results = AsyncMock()

    await processor.run()

    processor._load_year_data.assert_not_awaited()
    processor._compute_child_receptor_exposure.assert_not_awaited()
    processor._save_year_results.assert_not_awaited()
    processor.log.warning.assert_called_once()


def test_pesticide_year_cli_filter_is_applied():
    config = ChildReceptorExposureGoldConfig()

    config.apply_cli_filters(SimpleNamespace(pesticide_year=2022))

    assert config.pesticide_year == 2022
