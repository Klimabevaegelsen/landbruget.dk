"""Tests for Kemidata surface water silver processing."""

import zipfile
from pathlib import Path

from unified_pipeline.silver.kemidata_surface_water import (
    KemidataSurfaceWaterSilver,
    KemidataSurfaceWaterSilverConfig,
)


def test_load_csv_from_storage_downloads_via_storage_fs(tmp_path: Path) -> None:
    """Kemidata CSV manifests use storage paths that DuckDB cannot read directly."""
    csv_file = tmp_path / "kemidata_export.csv"
    csv_file.write_text("StationName;Value\nStation A;1\n", encoding="utf-8")

    silver = KemidataSurfaceWaterSilver(KemidataSurfaceWaterSilverConfig(save_local=True))
    opened: list[tuple[str, str]] = []

    def open_csv(path: str, mode: str):
        opened.append((path, mode))
        return csv_file.open(mode)

    silver.storage.fs.open.side_effect = open_csv

    table_name = silver._load_csv_from_storage("bronze/kemidata/test/kemidata_export.csv")

    assert table_name == "raw_kemidata"
    assert opened == [("landbruget-data/bronze/kemidata/test/kemidata_export.csv", "rb")]
    row = silver.conn.execute('SELECT "StationName", "Value" FROM raw_kemidata').fetchone()
    assert row == ("Station A", "1")


def test_load_csv_from_storage_extracts_zipped_kemidata_export(tmp_path: Path) -> None:
    """Kemidata bronze files can be ZIP payloads even when the manifest says CSV."""
    zip_file = tmp_path / "kemidata_export.csv"
    with zipfile.ZipFile(zip_file, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("Kemi.csv", "StationName;Value\nStation B;2\n")

    silver = KemidataSurfaceWaterSilver(KemidataSurfaceWaterSilverConfig(save_local=True))
    silver.storage.fs.open.side_effect = lambda _path, mode: zip_file.open(mode)

    table_name = silver._load_csv_from_storage("bronze/kemidata/test/kemidata_export.csv")

    assert table_name == "raw_kemidata"
    row = silver.conn.execute('SELECT "StationName", "Value" FROM raw_kemidata').fetchone()
    assert row == ("Station B", "2")


def test_transform_data_uses_stedtekst_for_station_geometry() -> None:
    """The live Kemidata export uses Stedtekst for the station name column."""
    silver = KemidataSurfaceWaterSilver(KemidataSurfaceWaterSilverConfig(save_local=True))
    silver.conn.execute("""
        CREATE OR REPLACE TABLE raw_kemidata AS
        SELECT
            'Station' AS "Stedtype",
            '53000010' AS "StedID",
            'LL. VEJLE Å, PILEMØLLEN' AS "Stedtekst",
            '708823' AS "x-koordinat",
            '6167762' AS "y-koordinat",
            'Vandløb' AS "Medie"
    """)

    table_name = silver._transform_data(
        [
            {
                "station_id": "615da609-389f-4d47-a024-71b0177a8da5",
                "station_name": "LL. VEJLE Å, PILEMØLLEN",
                "media_name": "Vandløb",
                "x": 708823.0,
                "y": 6167762.0,
            },
            {
                "station_id": "duplicate-name",
                "station_name": "LL. VEJLE Å, PILEMØLLEN",
                "media_name": "Vandløb",
                "x": 1.0,
                "y": 2.0,
            },
        ]
    )

    count, with_geometry, x_coord, y_coord = silver.conn.execute(
        f"""
        SELECT COUNT(*), COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END), MIN(x_coord), MIN(y_coord)
        FROM {table_name}
        """
    ).fetchone()
    assert count == 1
    assert with_geometry == 1
    assert (x_coord, y_coord) == (708823.0, 6167762.0)
