"""Regression tests for the INSPIRE to GeoDanmark building UUID join."""

import logging
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import duckdb
import pandas as pd
import pytest

import main as bbr_main
from bronze.bulk_geodanmark_graphql_fetcher import BulkGeoDanmarkGraphQLFetcher
from config.settings import Settings
from main import perform_uuid_join_optimized, run_silver_layer
from silver.building_processor import BuildingProcessor


def _write_geodanmark(
    output_dir: Path,
    bbr_uuids: list[str | None],
    *,
    crs_tag: str | None = None,
    longitude_latitude: bool = False,
) -> Path:
    """Write 10 m square polygons with synthetic GeoDanmark BBRUUID values."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "geodanmark_buildings_complete.geoparquet"
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute(
        "CREATE TABLE buildings (BBRUUID VARCHAR, bygningstype VARCHAR, geometri GEOMETRY)"
    )

    for index, bbr_uuid in enumerate(bbr_uuids):
        if longitude_latitude:
            x, y, size = 12.0 + index * 0.01, 55.0, 0.001
        else:
            x, y, size = 600_000 + index * 20, 6_200_000, 10
        polygon = (
            f"POLYGON (({x} {y}, {x + size} {y}, {x + size} {y + size}, {x} {y + size}, {x} {y}))"
        )
        if crs_tag:
            geometry_sql = "ST_SetCRS(ST_GeomFromText(?), ?)"
            values = [bbr_uuid, "building", polygon, crs_tag]
        else:
            geometry_sql = "ST_GeomFromText(?)"
            values = [bbr_uuid, "building", polygon]
        conn.execute(
            f"INSERT INTO buildings VALUES (?, ?, {geometry_sql})",
            values,
        )

    conn.execute(f"COPY buildings TO '{path}' (FORMAT PARQUET)")
    conn.close()
    return path


def _attributes(
    building_uuids: list[object],
    *,
    conditions: list[str | None] | None = None,
) -> pd.DataFrame:
    records = {
        "building_uuid": building_uuids,
        "current_use": ["agriculture"] * len(building_uuids),
        "building_nature": ["building"] * len(building_uuids),
        "construction_year": ["2000"] * len(building_uuids),
        "floor_area": [50.0] * len(building_uuids),
        "floors": [1] * len(building_uuids),
        "dwellings": [0] * len(building_uuids),
        "address": ["Testvej 1"] * len(building_uuids),
        "bbr_usage_code": ["210"] * len(building_uuids),
        "category_group": ["agriculture"] * len(building_uuids),
    }
    if conditions is not None:
        records["condition_of_construction"] = conditions
    return pd.DataFrame(records)


def _join(
    tmp_path: Path,
    bbr_uuids: list[str | None],
    attributes: pd.DataFrame,
    *,
    crs_tag: str | None = None,
    longitude_latitude: bool = False,
) -> tuple[dict, list[tuple[str, float]]]:
    geodanmark_path = _write_geodanmark(
        tmp_path / "geodanmark",
        bbr_uuids,
        crs_tag=crs_tag,
        longitude_latitude=longitude_latitude,
    )
    output_dir = tmp_path / "silver"
    output_dir.mkdir()
    result = perform_uuid_join_optimized(
        building_ids=[str(value) for value in attributes["building_uuid"]],
        geodanmark_path=str(geodanmark_path),
        output_dir=output_dir,
        attributes_df=attributes,
    )
    conn = duckdb.connect()
    conn.execute("LOAD spatial")
    rows = conn.execute(
        f"SELECT BBRUUID, building_area_m2 FROM read_parquet('{result['output_file']}')"
    ).fetchall()
    conn.close()
    return result, rows


def test_join_normalizes_uppercase_ids_skips_nulls_and_uses_planar_area(tmp_path: Path) -> None:
    uuids = [str(uuid4()) for _ in range(3)]
    attributes = _attributes([value.lower() for value in uuids])

    result, rows = _join(
        tmp_path,
        [uuids[0].upper(), uuids[1].upper(), uuids[2], None],
        attributes,
    )

    assert result["joined_buildings_count"] == 3
    assert {bbr_uuid for bbr_uuid, _ in rows} == {value.lower() for value in uuids}
    assert all(area == pytest.approx(100.0) for _, area in rows)


@pytest.mark.parametrize(
    "uuid_factory",
    [UUID, lambda value: UUID(value).bytes],
    ids=["uuid-objects", "16-byte-values"],
)
def test_join_normalizes_non_string_uuid_types(tmp_path: Path, uuid_factory) -> None:
    uuids = [uuid4() for _ in range(3)]
    attributes = _attributes([uuid_factory(str(value)) for value in uuids])

    result, rows = _join(
        tmp_path,
        [str(value).upper() for value in uuids],
        attributes,
    )

    assert result["joined_buildings_count"] == 3
    assert {bbr_uuid for bbr_uuid, _ in rows} == {str(value) for value in uuids}


def test_demolished_attributes_are_excluded_before_matching(tmp_path: Path) -> None:
    active_uuid, demolished_uuid = str(uuid4()), str(uuid4())
    attributes = _attributes(
        [active_uuid, demolished_uuid], conditions=["functional", "demolished"]
    )

    result, rows = _join(tmp_path, [active_uuid, demolished_uuid], attributes)

    assert result["joined_buildings_count"] == 1
    assert rows[0][0] == active_uuid


def test_join_supports_attributes_without_construction_condition(tmp_path: Path) -> None:
    building_uuid = str(uuid4())
    result, rows = _join(tmp_path, [building_uuid], _attributes([building_uuid]))

    assert result["joined_buildings_count"] == 1
    assert rows[0][0] == building_uuid


def test_join_raises_when_match_rate_is_below_threshold(tmp_path: Path) -> None:
    uuids = [str(uuid4()) for _ in range(10)]
    geodanmark_path = _write_geodanmark(tmp_path / "geodanmark", uuids[:8])
    output_dir = tmp_path / "silver"
    output_dir.mkdir()

    with pytest.raises(RuntimeError, match="overall match rate"):
        perform_uuid_join_optimized(
            building_ids=uuids,
            geodanmark_path=str(geodanmark_path),
            output_dir=output_dir,
            attributes_df=_attributes(uuids),
        )


def test_crs84_tagged_projected_geometry_keeps_planar_area(tmp_path: Path) -> None:
    building_uuid = str(uuid4())
    _, rows = _join(
        tmp_path,
        [building_uuid],
        _attributes([building_uuid]),
        crs_tag="OGC:CRS84",
    )

    assert rows[0][1] == pytest.approx(100.0)


def test_lon_lat_geometry_uses_spheroidal_area_fallback(tmp_path: Path) -> None:
    building_uuid = str(uuid4())
    _, rows = _join(
        tmp_path,
        [building_uuid],
        _attributes([building_uuid]),
        longitude_latitude=True,
    )

    assert rows[0][1] == pytest.approx(12_047.0, rel=0.01)


def test_geodanmark_bronze_batch_and_combined_files_use_epsg_25832(tmp_path: Path) -> None:
    fetcher = BulkGeoDanmarkGraphQLFetcher.__new__(BulkGeoDanmarkGraphQLFetcher)
    fetcher.output_dir = tmp_path
    fetcher.conn = duckdb.connect()
    fetcher.conn.execute("LOAD spatial")
    fetcher.conn.execute("""
        CREATE TABLE page AS
        SELECT
            'building-id' AS BBRUUID,
            'building' AS bygningstype,
            ST_GeomFromText(
                'POLYGON ((600000 6200000, 600010 6200000, 600010 6200010, '
                '600000 6200010, 600000 6200000))'
            ) AS geometri
    """)

    fetcher._save_intermediate_results(["page"], batch_num=0)
    batch_path = tmp_path / "geodanmark_buildings_batch_0000.geoparquet"
    batch_crs = fetcher.conn.execute(
        f"SELECT ST_CRS(geometri) FROM read_parquet('{batch_path}')"
    ).fetchone()[0]
    assert batch_crs == "EPSG:25832"

    fetcher._combine_intermediate_files()
    complete_path = tmp_path / "geodanmark_buildings_complete.geoparquet"
    complete_crs = fetcher.conn.execute(
        f"SELECT ST_CRS(geometri) FROM read_parquet('{complete_path}')"
    ).fetchone()[0]
    assert complete_crs == "EPSG:25832"
    fetcher.conn.close()


def test_silver_stops_when_inspire_attributes_are_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    upload_calls = []
    monkeypatch.setattr(bbr_main, "_load_bronze_data", lambda *_args: (["building-id"], None))
    monkeypatch.setattr(
        bbr_main,
        "_load_geodanmark_data_from_storage",
        lambda *_args: pytest.fail("GeoDanmark should not load without INSPIRE attributes"),
    )
    monkeypatch.setattr(
        bbr_main, "_upload_silver_data_to_storage", lambda *args: upload_calls.append(args)
    )

    result = run_silver_layer(
        SimpleNamespace(output_dir=tmp_path),
        Settings(),
        logging.getLogger("test.bbr.silver"),
    )

    assert result is None
    assert upload_calls == []


def test_building_processor_rejects_join_without_inspire_columns(tmp_path: Path) -> None:
    input_dir = tmp_path / "silver"
    input_dir.mkdir()
    joined_path = input_dir / "joined_buildings.geoparquet"
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute(f"""
        COPY (
            SELECT
                'building-id' AS BBRUUID,
                ST_GeomFromText(
                    'POLYGON ((600000 6200000, 600010 6200000, 600010 6200010, '
                    '600000 6200010, 600000 6200000))'
                ) AS geometry,
                'building' AS bygningstype,
                100.0 AS building_area_m2,
                'direct_uuid_matched' AS join_status
        ) TO '{joined_path}' (FORMAT PARQUET)
    """)
    conn.close()

    processor = BuildingProcessor(Settings(), logging.getLogger("test.bbr.processor"))
    with pytest.raises(ValueError, match="missing required INSPIRE columns"):
        processor.process_buildings(input_dir, tmp_path / "processed")
