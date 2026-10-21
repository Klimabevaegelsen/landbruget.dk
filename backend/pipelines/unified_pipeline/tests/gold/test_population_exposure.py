"""Tests for population exposure distances, allocation, and summary output."""

from time import perf_counter

import duckdb
import pytest

from unified_pipeline.gold.population_exposure import (
    DEFAULT_BANDS_M,
    PopulationExposureGold,
    PopulationExposureGoldConfig,
    allocate_population,
    assign_municipalities,
    build_population_summary,
    compute_building_field_distances,
)


@pytest.fixture
def spatial_connection():
    conn = duckdb.connect(database=":memory:")
    conn.execute("LOAD spatial")
    yield conn
    conn.close()


def _create_scene(conn):
    """Two municipalities, four 10 m residential buildings, and three fields."""
    conn.execute("""
        CREATE TABLE buildings (
            building_uuid VARCHAR,
            category_group VARCHAR,
            inspire_dwellings VARCHAR,
            geometry GEOMETRY
        )
    """)
    base_e = 600000.0
    base_n = 6200000.0
    for building_uuid, x, dwellings in [
        ("B1", 0.0, "2"),
        ("B2", 100.0, "1"),
        ("B3", 200.0, "2"),
        # Outside both municipality polygons, within 5 km of Kommune B, and
        # beyond the 1 km field search radius.
        ("B4", 1500.0, "1"),
    ]:
        conn.execute(
            "INSERT INTO buildings VALUES (?, 'residential', ?, ST_MakeEnvelope(?, ?, ?, ?))",
            (building_uuid, dwellings, base_e + x, base_n, base_e + x + 10, base_n + 10),
        )
    conn.execute("""
        CREATE TABLE fields (
            field_uuid VARCHAR,
            is_organic BOOLEAN,
            is_treated BOOLEAN,
            geometry GEOMETRY
        )
    """)
    conn.execute("""
        INSERT INTO fields VALUES
            ('A', false, true, ST_MakeEnvelope(600020, 6200000, 600030, 6200010)),
            ('B', true, false, ST_MakeEnvelope(600050, 6200000, 600060, 6200010)),
            ('C', false, false, ST_MakeEnvelope(600130, 6200000, 600140, 6200010)),
            (NULL, NULL, false, ST_MakeEnvelope(600015, 6200000, 600019, 6200010))
    """)
    conn.execute("""
        CREATE TABLE kommuner (code VARCHAR, name VARCHAR, geometry GEOMETRY)
    """)
    conn.execute("""
        INSERT INTO kommuner VALUES
            ('0101', 'Kommune A', ST_MakeEnvelope(599900, 6199900, 600120, 6200100)),
            ('0102', 'Kommune B', ST_MakeEnvelope(600120, 6199900, 600300, 6200100))
    """)


def test_distances_municipality_assignment_allocation_and_summary(spatial_connection):
    conn = spatial_connection
    _create_scene(conn)

    distance_table = compute_building_field_distances(
        conn, "buildings", "fields", max_distance_m=1000, batch_size=2
    )
    assigned_table = assign_municipalities(conn, distance_table, "kommuner")
    population = {101: 100, 102: 200, 103: 50}
    allocation = allocate_population(conn, assigned_table, population)

    distances = {
        row[0]: row[1:]
        for row in conn.execute(f"""
            SELECT building_uuid, dist_any_field_m, dist_conventional_field_m,
                   dist_treated_field_m, kommune_code, dwellings, est_persons
            FROM {assigned_table}
            ORDER BY building_uuid
        """).fetchall()
    }
    assert distances["B1"] == (5.0, 5.0, 10.0, "0101", 2, pytest.approx(200 / 3))
    assert distances["B2"] == (20.0, 20.0, 70.0, "0101", 1, pytest.approx(100 / 3))
    assert distances["B3"] == (60.0, 60.0, 170.0, "0102", 2, pytest.approx(400 / 3))
    assert distances["B4"] == (None, None, None, "0102", 1, pytest.approx(200 / 3))
    assert allocation["population_unallocated"] == 50
    assert allocation["allocated_population"] == pytest.approx(300)

    building_meta = {
        "residential_buildings": 4,
        "dwellings": 6,
        "buildings_without_kommune": 0,
        "dwellings_without_kommune": 0,
    }
    summary = build_population_summary(
        conn,
        assigned_table,
        bands=[10, 30, 50, 100, 250, 500, 1000],
        pesticide_year=2024,
        population_meta={"period": "2026K3", "total_population": 350, **allocation},
        building_meta=building_meta,
        kommune_names={101: "Kommune A", 102: "Kommune B"},
        kommune_population=population,
    )

    assert set(summary) == {
        "schema_version",
        "generated_at",
        "pesticide_year",
        "field_year",
        "max_distance_m",
        "bands_m",
        "population_source",
        "buildings",
        "national",
        "municipalities",
    }
    assert summary["schema_version"] == 1
    assert summary["field_year"] == 2025
    assert summary["bands_m"] == DEFAULT_BANDS_M
    assert summary["buildings"] == building_meta
    assert summary["population_source"] == {
        "provider": "Danmarks Statistik",
        "table": "FOLK1A",
        "period": "2026K3",
        "total_population": 350,
        "population_unallocated": 50,
    }
    national = summary["national"]
    assert national["any_field"][0] == {
        "band_m": 10,
        "dwellings": 2,
        "persons": 67,
        "share_of_population": 0.191429,
    }
    assert national["any_field"][3] == {
        "band_m": 100,
        "dwellings": 5,
        "persons": 233,
        "share_of_population": 0.665714,
    }
    assert national["treated_field"][1]["dwellings"] == 2
    assert national["treated_field"][1]["persons"] == 67
    assert national["any_field"][-1]["dwellings"] == 5
    assert national["any_field"][-1]["persons"] == 233
    municipalities = summary["municipalities"]
    assert [row["code"] for row in municipalities] == ["0101", "0102", "0103"]
    assert municipalities[0]["any_field"][1] == {
        "band_m": 30,
        "dwellings": 3,
        "persons": 100,
        "share_of_population": 1.0,
    }
    assert municipalities[1]["any_field"][3]["persons"] == 133
    assert municipalities[2]["any_field"][-1] == {
        "band_m": 1000,
        "dwellings": 0,
        "persons": 0,
        "share_of_population": 0.0,
    }
    assert set(municipalities[0]) == {
        "code",
        "name",
        "population",
        "any_field",
        "conventional_field",
        "treated_field",
    }


def test_batch_size_does_not_change_distances(spatial_connection):
    conn = spatial_connection
    _create_scene(conn)
    small_batch = compute_building_field_distances(conn, "buildings", "fields", 1000, 1)
    one_at_a_time = conn.execute(f"""
        SELECT building_uuid, dist_any_field_m, dist_conventional_field_m,
               dist_treated_field_m
        FROM {small_batch} ORDER BY building_uuid
    """).fetchall()
    large_batch = compute_building_field_distances(conn, "buildings", "fields", 1000, 1000)
    many_at_once = conn.execute(f"""
        SELECT building_uuid, dist_any_field_m, dist_conventional_field_m,
               dist_treated_field_m
        FROM {large_batch} ORDER BY building_uuid
    """).fetchall()
    assert many_at_once == one_at_a_time


def test_duplicate_building_rows_are_unioned_and_counted_once(spatial_connection):
    conn = spatial_connection
    conn.execute("""
        CREATE TABLE duplicate_buildings (
            building_uuid VARCHAR,
            category_group VARCHAR,
            inspire_dwellings VARCHAR,
            geometry GEOMETRY
        )
    """)
    conn.execute("""
        INSERT INTO duplicate_buildings VALUES
            ('duplicate', 'residential', '3', ST_MakeEnvelope(600000, 6200000, 600010, 6200010)),
            ('duplicate', 'residential', '3', ST_MakeEnvelope(600010, 6200000, 600020, 6200010))
    """)
    conn.execute("""
        CREATE TABLE fields (
            field_uuid VARCHAR,
            is_organic BOOLEAN,
            is_treated BOOLEAN,
            geometry GEOMETRY
        )
    """)
    conn.execute("""
        INSERT INTO fields VALUES
            ('field', false, true, ST_MakeEnvelope(600030, 6200000, 600040, 6200010))
    """)

    table = compute_building_field_distances(conn, "duplicate_buildings", "fields", 100, 500)

    assert conn.execute(
        f"SELECT COUNT(*), SUM(dwellings) FROM {table} WHERE building_uuid = 'duplicate'"
    ).fetchone() == (1, 3)


def test_spatial_batch_performance_and_result_equivalence(spatial_connection):
    conn = spatial_connection
    conn.execute("""
        CREATE TABLE performance_buildings AS
        SELECT
            'B' || CAST(i AS VARCHAR) AS building_uuid,
            'residential' AS category_group,
            '1' AS inspire_dwellings,
            ST_MakeEnvelope(
                600000 + (i % 75) * 200,
                6200000 + (i // 75) * 200,
                600010 + (i % 75) * 200,
                6200010 + (i // 75) * 200
            ) AS geometry
        FROM range(3000) AS buildings(i)
    """)
    conn.execute("""
        CREATE TABLE performance_fields AS
        SELECT
            'F' || CAST(i AS VARCHAR) AS field_uuid,
            i % 2 = 0 AS is_organic,
            i % 3 = 0 AS is_treated,
            ST_MakeEnvelope(
                600100 + (i % 25) * 600,
                6200100 + (i // 25) * 600,
                600120 + (i % 25) * 600,
                6200120 + (i // 25) * 600
            ) AS geometry
        FROM range(500) AS fields(i)
    """)

    started_at = perf_counter()
    small_batch_table = compute_building_field_distances(
        conn, "performance_buildings", "performance_fields", 1000, 500
    )
    small_batch_seconds = perf_counter() - started_at
    assert small_batch_seconds < 20, f"500-building batches took {small_batch_seconds:.2f}s"
    small_batch_rows = conn.execute(f"""
        SELECT building_uuid, dwellings, dist_any_field_m,
               dist_conventional_field_m, dist_treated_field_m
        FROM {small_batch_table}
        ORDER BY building_uuid
    """).fetchall()

    large_batch_table = compute_building_field_distances(
        conn, "performance_buildings", "performance_fields", 1000, 100000
    )
    large_batch_rows = conn.execute(f"""
        SELECT building_uuid, dwellings, dist_any_field_m,
               dist_conventional_field_m, dist_treated_field_m
        FROM {large_batch_table}
        ORDER BY building_uuid
    """).fetchall()

    assert large_batch_rows == small_batch_rows


def test_wgs84_building_geometry_is_transformed_before_distance_math(spatial_connection):
    conn = spatial_connection
    conn.execute("""
        CREATE TABLE buildings (
            building_uuid VARCHAR,
            category_group VARCHAR,
            inspire_dwellings VARCHAR,
            geometry GEOMETRY
        )
    """)
    conn.execute("""
        INSERT INTO buildings
        SELECT 'B1', 'residential', '1',
               ST_Transform(
                   ST_MakeEnvelope(600000, 6200000, 600010, 6200010),
                   'EPSG:25832', 'EPSG:4326', always_xy := true
               )
    """)
    conn.execute("""
        CREATE TABLE fields (
            field_uuid VARCHAR,
            is_organic BOOLEAN,
            is_treated BOOLEAN,
            geometry GEOMETRY
        )
    """)
    conn.execute("""
        INSERT INTO fields VALUES
        ('A', false, true, ST_MakeEnvelope(600020, 6200000, 600030, 6200010))
    """)

    table = compute_building_field_distances(conn, "buildings", "fields", 100, 5000)

    row = conn.execute(f"SELECT dist_any_field_m, utm_e, utm_n FROM {table}").fetchone()
    assert row[0] == pytest.approx(10.0, abs=0.02)
    assert row[1] == pytest.approx(600005.0, abs=0.1)
    assert row[2] == pytest.approx(6200005.0, abs=0.1)


def test_config_defaults_and_cli_year_filter():
    config = PopulationExposureGoldConfig()
    assert config.pesticide_year is None
    assert config.max_distance_m == 1000.0
    assert config.bands_m == DEFAULT_BANDS_M
    assert config.batch_size == 5000

    config.apply_cli_filters(type("CLI", (), {"pesticide_year": 2022})())
    assert config.pesticide_year == 2022


@pytest.mark.asyncio
async def test_year_discovery_skips_years_without_field_silver():
    processor = object.__new__(PopulationExposureGold)
    processor.config = PopulationExposureGoldConfig()
    processor.log = type("Log", (), {"info": lambda *args: None, "warning": lambda *args: None})()
    processor.storage = type(
        "Storage",
        (),
        {
            "list_files": lambda self, pattern: [
                "bucket/gold/pesticide_disaggregation_2022_2023/20261020/data.parquet",
                "bucket/gold/pesticide_disaggregation_2023_2024/20261020/data.parquet",
            ]
        },
    )()
    processor._read_silver_data = lambda dataset: {
        "bbr_buildings": "buildings",
        "dagi_kommuner": "kommuner",
        "fvm_marker_2023": "fields_2023",
        "fvm_marker_2024": None,
    }.get(dataset)

    datasets = await processor._load_datasets()

    assert datasets["disaggregation"] == {
        2022: "bucket/gold/pesticide_disaggregation_2022_2023/20261020/data.parquet"
    }
    assert datasets["years"] == [2022]
    assert datasets["field_tables"] == {2022: "fields_2023"}
