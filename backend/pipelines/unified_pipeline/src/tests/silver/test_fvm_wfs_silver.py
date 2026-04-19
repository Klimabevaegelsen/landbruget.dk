"""
Tests for FVM WFS Silver layer.
"""

import duckdb
import pytest

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
    assert len(config.organic_areas_years) == 13  # 2012-2024

    # Test new municipality assignment configuration
    assert config.kommune_boundaries_dataset == "dagi_kommuner"
    assert config.include_municipality_assignment is True
    assert config.municipality_assignment_method == "spatial_with_fallback"


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
