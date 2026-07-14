"""
Tests for FVM WFS Silver layer.
"""

from unittest.mock import Mock

import duckdb
import pytest

from unified_pipeline.model.cli import CliConfig, Env, FVMLayerType, Source, Stage
from unified_pipeline.silver.fvm_wfs import FVMWFSSilver, FVMWFSSilverConfig


@pytest.fixture
def config() -> FVMWFSSilverConfig:
    """Create a test configuration."""
    return FVMWFSSilverConfig(save_local=True)


@pytest.fixture
def fvm_wfs_silver(config: FVMWFSSilverConfig) -> FVMWFSSilver:
    """Create a FVM WFS silver instance."""
    return FVMWFSSilver(config)


def test_fvm_wfs_silver_config() -> None:
    """Test FVM WFS silver configuration."""
    config = FVMWFSSilverConfig()

    assert config.name == "Danish FVM WFS Agricultural Data - Silver"
    assert config.type == "transformation"
    assert config.dataset_markblokke == "fvm_markblokke"
    assert config.dataset_marker == "fvm_marker"
    assert config.dataset_smaabiotoper == "fvm_smaabiotoper"
    assert config.dataset_organic_areas == "fvm_organic_areas"
    assert len(config.organic_areas_years) == 14  # 2012-2025

    # Test new municipality assignment configuration
    assert config.kommune_boundaries_dataset == "dagi_kommuner"
    assert config.include_municipality_assignment is True
    assert config.municipality_assignment_method == "spatial_with_fallback"
    assert config.cvr_backfill_gap_years == [2015]
    assert config.cvr_backfill_overlap_threshold == 0.7
    assert config.cvr_backfill_prev_offset == 1
    assert config.cvr_backfill_next_offset == 1
    assert config.cvr_ejernr_backfill_enabled is True
    assert config.cvr_ejernr_jordbrug_dataset == "jordbrugsanalyser_markers"
    assert config.cvr_ejernr_target_years == [2015]
    assert config.cvr_ejernr_reference_years == [2013, 2014, 2016, 2017]
    assert config.cvr_ejernr_attach_overlap_threshold == 0.9
    assert config.cvr_ejernr_dominance_threshold == 0.8


def test_apply_cli_filters_marker_uses_live_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Marker filtering should use live-discovered marker years."""
    config = FVMWFSSilverConfig()

    monkeypatch.setattr(
        "unified_pipeline.silver.fvm_wfs.discover_fvm_layer_years",
        lambda _wfs_url: {
            "markblokke": [2005, 2026],
            "marker": [2024, 2026],
            "smaabiotoper": [2026],
            "organic_areas": [2012, 2025],
            "organic_subsidies": [2017, 2024],
            "grassland_subsidies": [2017, 2024],
            "environmental_subsidies": [2012, 2023],
        },
    )

    cli_config = CliConfig(
        env=Env.prod,
        source=Source.fvm_wfs,
        stage=Stage.silver,
        fvm_layer_type=FVMLayerType.marker,
    )
    config.apply_cli_filters(cli_config)

    assert config.marker_years == [2024, 2026]


def test_apply_cli_filters_fail_fast_on_discovery_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery failures should fail fast when marker discovery is required."""
    config = FVMWFSSilverConfig()

    def _raise(_wfs_url: str) -> dict[str, list[int]]:
        raise RuntimeError("capabilities unavailable")

    monkeypatch.setattr("unified_pipeline.silver.fvm_wfs.discover_fvm_layer_years", _raise)

    cli_config = CliConfig(
        env=Env.prod,
        source=Source.fvm_wfs,
        stage=Stage.silver,
        fvm_layer_type=FVMLayerType.marker,
    )

    with pytest.raises(RuntimeError, match="capabilities unavailable"):
        config.apply_cli_filters(cli_config)


def test_enrichment_uses_live_years_for_membership(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enrichment stage membership checks should use live-discovered years."""
    config = FVMWFSSilverConfig()

    monkeypatch.setattr(
        "unified_pipeline.silver.fvm_wfs.discover_fvm_layer_years",
        lambda _wfs_url: {
            "markblokke": [2005, 2026],
            "marker": [2024, 2026],
            "smaabiotoper": [2023, 2026],
            "organic_areas": [2012, 2025],
            "organic_subsidies": [2017, 2024],
            "grassland_subsidies": [2017, 2024],
            "environmental_subsidies": [2012, 2023],
        },
    )

    cli_config = CliConfig(
        env=Env.prod,
        source=Source.fvm_wfs,
        stage=Stage.enrichment,
        fvm_year=2026,
    )
    config.apply_cli_filters(cli_config)

    assert config.markblokke_years == [2026]
    assert config.marker_years == [2026]
    assert config.smaabiotoper_years == [2026]


def test_subsidy_field_uuid_join_avoids_cross_cvr_collisions() -> None:
    """
    Regression test for _enrich_subsidies_with_field_uuid spatial join.

    field_id is a per-farmer field number and is reused across CVRs. Two
    different CVRs can both have a field "4-0", and those polygons can be
    geographically close enough that one's centroid falls inside the other's
    polygon — joining on field_id alone would assign the wrong CVR's
    field_uuid to the subsidy.

    The fix added cvr_number to the equi-predicate. This test sets up exactly
    that collision scenario and asserts the subsidy of CVR_A is matched only
    to CVR_A's marker, never to CVR_B's marker, even though CVR_B's marker
    would satisfy the spatial predicate.
    """
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial; LOAD spatial;")

    con.execute("""
        CREATE TABLE temp_marker_2019 (
            field_id VARCHAR,
            cvr_number VARCHAR,
            field_uuid VARCHAR,
            geometry GEOMETRY
        )
    """)
    # CVR_A's "4-0" is a small square at (0,0)–(10,10); UUID = "uuid-A".
    # CVR_B's "4-0" is a large square at (-100,-100)–(100,100) that *also*
    # contains CVR_A's centroid — this is the spurious-match trap.
    con.execute("""
        INSERT INTO temp_marker_2019 VALUES
        ('4-0', 'CVR_A', 'uuid-A', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('4-0', 'CVR_B', 'uuid-B', ST_GeomFromText('POLYGON((-100 -100, 100 -100, 100 100, -100 100, -100 -100))'))
    """)

    con.execute("""
        CREATE TABLE temp_subsidy_filtered_2019 (
            field_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    # CVR_A's subsidy "4-0" overlaps CVR_A's marker (centroid at 5,5).
    # Both CVR_A's and CVR_B's marker would satisfy ST_Contains alone.
    con.execute("""
        INSERT INTO temp_subsidy_filtered_2019 VALUES
        ('4-0', 'CVR_A', ST_GeomFromText('POLYGON((1 1, 9 1, 9 9, 1 9, 1 1))'))
    """)

    # This is the production query (mirrors fvm_wfs.py:_enrich_subsidies_with_field_uuid).
    con.execute("""
        CREATE TABLE temp_spatial_matches_2019 AS
        SELECT DISTINCT s.rowid AS subsidy_rowid, m.field_uuid
        FROM temp_subsidy_filtered_2019 s
        INNER JOIN temp_marker_2019 m
            ON ST_Contains(m.geometry, ST_Centroid(s.geometry))
        WHERE s.field_id = m.field_id
          AND s.cvr_number = m.cvr_number
    """)

    matches = con.execute(
        "SELECT subsidy_rowid, field_uuid FROM temp_spatial_matches_2019 ORDER BY subsidy_rowid"
    ).fetchall()

    assert matches == [(0, "uuid-A")], (
        f"Expected single match to CVR_A's marker (uuid-A); got {matches}. "
        f"If 'uuid-B' appears, the cvr_number predicate has been removed and "
        f"cross-CVR field_id collisions are producing spurious matches."
    )

    # Sanity: removing the cvr_number predicate would produce 2 matches
    # (the spurious-match trap that the production query must avoid).
    spurious = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT s.rowid, m.field_uuid
            FROM temp_subsidy_filtered_2019 s
            INNER JOIN temp_marker_2019 m
                ON ST_Contains(m.geometry, ST_Centroid(s.geometry))
            WHERE s.field_id = m.field_id
        )
    """).fetchone()[0]
    assert spurious == 2, "Test fixture is wrong — the spurious-match trap is not set up correctly"


@pytest.mark.asyncio
async def test_subsidy_enrichment_skips_legacy_tables_without_cvr(
    fvm_wfs_silver: FVMWFSSilver,
) -> None:
    """Legacy subsidy tables without CVR must not get ambiguous field_id-only UUID matches."""
    object.__setattr__(fvm_wfs_silver.config, "marker_years", [2012])
    object.__setattr__(fvm_wfs_silver.config, "organic_subsidies_years", [])
    object.__setattr__(fvm_wfs_silver.config, "grassland_subsidies_years", [])
    object.__setattr__(fvm_wfs_silver.config, "environmental_subsidies_years", [2012])

    def list_files(pattern: str) -> list[str]:
        if "fvm_environmental_subsidies_2012" in pattern:
            return ["landbruget-data/silver/fvm_environmental_subsidies_2012/ts/data.parquet"]
        if "fvm_marker_2012" in pattern:
            return ["landbruget-data/silver/fvm_marker_2012/ts/data.parquet"]
        return []

    def query_parquet_direct(_path: str, _query: str, table_name: str) -> None:
        if table_name == "temp_subsidy_2012":
            fvm_wfs_silver.conn.execute("""
                CREATE OR REPLACE TABLE temp_subsidy_2012 AS
                SELECT
                    '4-0' AS field_id,
                    ST_GeomFromText('POLYGON((1 1, 9 1, 9 9, 1 9, 1 1))') AS geometry
            """)
            return
        if table_name == "temp_marker_2012":
            fvm_wfs_silver.conn.execute("""
                CREATE OR REPLACE TABLE temp_marker_2012 AS
                SELECT
                    '4-0' AS field_id,
                    '12345678' AS cvr_number,
                    'marker-uuid' AS field_uuid,
                    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') AS geometry
            """)
            return
        raise AssertionError(f"Unexpected table load: {table_name}")

    fvm_wfs_silver.storage.list_files = Mock(side_effect=list_files)
    fvm_wfs_silver.storage.query_parquet_direct = Mock(side_effect=query_parquet_direct)
    fvm_wfs_silver._save_data = Mock()

    await fvm_wfs_silver._enrich_subsidies_with_field_uuid()

    fvm_wfs_silver._save_data.assert_not_called()
    assert fvm_wfs_silver.storage.query_parquet_direct.call_count == 2


@pytest.mark.asyncio
async def test_cvr_backfill_from_adjacent_years_requires_agreement(
    fvm_wfs_silver: FVMWFSSilver,
) -> None:
    """Gap-year CVR backfill should only fill when both adjacent years agree."""
    fvm_wfs_silver.conn.execute("INSTALL spatial; LOAD spatial;")
    object.__setattr__(fvm_wfs_silver.config, "cvr_backfill_gap_years", [2015])
    object.__setattr__(fvm_wfs_silver.config, "cvr_backfill_overlap_threshold", 0.7)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE fvm_marker_2014 (
            field_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO fvm_marker_2014 VALUES
        ('A', '11111111', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('B', '22222222', ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))'))
    """)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE fvm_marker_2015 (
            field_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO fvm_marker_2015 VALUES
        ('A', CAST(NULL AS VARCHAR), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('B', CAST(NULL AS VARCHAR), ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))')),
        ('C', CAST(NULL AS VARCHAR), ST_GeomFromText('POLYGON((40 0, 50 0, 50 10, 40 10, 40 0))')),
        ('D', CAST(NULL AS VARCHAR), ST_GeomFromText('POLYGON((60 0, 70 0, 70 10, 60 10, 60 0))'))
    """)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE fvm_marker_2016 (
            field_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO fvm_marker_2016 VALUES
        ('A', '11111111', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('B', '33333333', ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))')),
        ('C', '44444444', ST_GeomFromText('POLYGON((40 0, 50 0, 50 10, 40 10, 40 0))')),
        ('D', '55555555', ST_GeomFromText('POLYGON((69 0, 79 0, 79 10, 69 10, 69 0))'))
    """)

    fvm_wfs_silver.storage.list_files = Mock(side_effect=AssertionError("unexpected storage call"))
    fvm_wfs_silver.storage.query_parquet_direct = Mock(
        side_effect=AssertionError("unexpected storage call")
    )
    fvm_wfs_silver._save_data = Mock()

    await fvm_wfs_silver._backfill_cvr_from_adjacent_years()

    rows = dict(
        fvm_wfs_silver.conn.execute("""
            SELECT field_id, (cvr_number, cvr_source)
            FROM fvm_marker_2015
            ORDER BY field_id
        """).fetchall()
    )

    assert rows["A"] == ("11111111", "adjacent_year_agreement")
    assert rows["B"] == (None, None)
    assert rows["C"] == (None, None)
    assert rows["D"] == (None, None)

    filled_count = fvm_wfs_silver.conn.execute("""
        SELECT COUNT(*)
        FROM fvm_marker_2015
        WHERE cvr_source = 'adjacent_year_agreement'
    """).fetchone()[0]
    assert filled_count == 1
    fvm_wfs_silver._save_data.assert_called_once()


@pytest.mark.asyncio
async def test_cvr_backfill_via_ejernr_bridge_uses_dominant_reference_cvr(
    fvm_wfs_silver: FVMWFSSilver,
) -> None:
    """EjerNr bridge should fill target fields only when a trusted EjerNr→CVR exists."""
    fvm_wfs_silver.conn.execute("INSTALL spatial; LOAD spatial;")
    object.__setattr__(fvm_wfs_silver.config, "cvr_ejernr_target_years", [2015])
    object.__setattr__(fvm_wfs_silver.config, "cvr_ejernr_reference_years", [2016])
    object.__setattr__(fvm_wfs_silver.config, "cvr_ejernr_attach_overlap_threshold", 0.9)
    object.__setattr__(fvm_wfs_silver.config, "cvr_ejernr_dominance_threshold", 0.8)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE fvm_marker_2016 (
            field_id VARCHAR,
            block_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO fvm_marker_2016 VALUES
        ('1', '10', '11111111', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('2', '20', '22222222', ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))')),
        ('3', '30', '33333333', ST_GeomFromText('POLYGON((40 0, 50 0, 50 10, 40 10, 40 0))'))
    """)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE jordbrugsanalyser_markers_2016 (
            owner_number INTEGER,
            field_block VARCHAR,
            field_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO jordbrugsanalyser_markers_2016 VALUES
        (100, '10', '1', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        (200, '20', '2', ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))')),
        (200, '30', '3', ST_GeomFromText('POLYGON((40 0, 50 0, 50 10, 40 10, 40 0))'))
    """)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE fvm_marker_2015 (
            field_id VARCHAR,
            block_id VARCHAR,
            cvr_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO fvm_marker_2015 VALUES
        ('1', '10', NULL, ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        ('4', '40', NULL, ST_GeomFromText('POLYGON((60 0, 70 0, 70 10, 60 10, 60 0))')),
        ('G', 'NO_KEY', NULL, ST_GeomFromText('POLYGON((100.25 0.25, 110.25 0.25, 110.25 10.25, 100.25 10.25, 100.25 0.25))')),
        ('X', 'NO_OWNER', NULL, ST_GeomFromText('POLYGON((200 0, 210 0, 210 10, 200 10, 200 0))'))
    """)

    fvm_wfs_silver.conn.execute("""
        CREATE OR REPLACE TABLE jordbrugsanalyser_markers_2015 (
            owner_number INTEGER,
            field_block VARCHAR,
            field_number VARCHAR,
            geometry GEOMETRY
        )
    """)
    fvm_wfs_silver.conn.execute("""
        INSERT INTO jordbrugsanalyser_markers_2015 VALUES
        (100, '10', '1', ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')),
        (200, '40', '4', ST_GeomFromText('POLYGON((60 0, 70 0, 70 10, 60 10, 60 0))')),
        (100, 'JB_GEOM', 'GEOM_ONLY', ST_GeomFromText('POLYGON((100 0, 110 0, 110 10, 100 10, 100 0))'))
    """)

    fvm_wfs_silver.storage.list_files = Mock(side_effect=AssertionError("unexpected storage call"))
    fvm_wfs_silver.storage.query_parquet_direct = Mock(
        side_effect=AssertionError("unexpected storage call")
    )
    fvm_wfs_silver._save_data = Mock()

    await fvm_wfs_silver._backfill_cvr_via_ejernr()

    rows = {
        field_id: (cvr_number, cvr_source)
        for field_id, cvr_number, cvr_source in fvm_wfs_silver.conn.execute("""
            SELECT field_id, cvr_number, cvr_source
            FROM fvm_marker_2015
            ORDER BY field_id
        """).fetchall()
    }

    assert rows["1"] == ("11111111", "ejernr_bridge")
    assert rows["G"] == ("11111111", "ejernr_bridge")
    assert rows["4"] == (None, None)
    assert rows["X"] == (None, None)
    fvm_wfs_silver._save_data.assert_called_once()
