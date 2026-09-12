"""Regression tests for in-depth Gødningsregnskab parsing."""

from types import SimpleNamespace

import duckdb
import pytest

from unified_pipeline.gold.nles5_nitrogen_estimation.data_loader import NLES5DataLoader
from unified_pipeline.gold.nles5_nitrogen_estimation.fertilizer_distributor import (
    NLES5FertilizerDistributor,
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


class _Storage:
    def __init__(self, connection: duckdb.DuckDBPyConnection, paths: list[str] | None = None):
        self.connection = connection
        self.paths = paths or []

    def list_files(self, _pattern: str) -> list[str]:
        return self.paths

    def create_table_from_storage(self, table_name: str, path: str) -> None:
        self.connection.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')"
        )


class _MappedGkeaStorage(_Storage):
    """Map a canonical cloud path to a local fixture for path-boundary tests."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, fixture_path: str):
        super().__init__(connection)
        self.fixture_path = fixture_path
        self.created_paths: list[str] = []

    def create_table_from_storage(self, table_name: str, path: str) -> None:
        self.created_paths.append(path)
        self.connection.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{self.fixture_path}')"
        )


def _loader(
    connection: duckdb.DuckDBPyConnection, storage: _Storage | None = None
) -> NLES5DataLoader:
    processor = SimpleNamespace(
        config=SimpleNamespace(bucket="landbruget-data", fertilizer_dataset="fertiliser"),
        log=_Log(),
        storage_access=storage,
        conn=connection,
    )
    return NLES5DataLoader(processor)


def test_main_register_parts_are_aggregated_with_danish_number_format() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE raw_register (
            CVR VARCHAR,
            F_901 VARCHAR,
            F_706_1 VARCHAR,
            F_704_1 VARCHAR,
            F_318_1 VARCHAR,
            F_308_1 VARCHAR,
            F_243 VARCHAR,
            F_512 VARCHAR,
            F_902 VARCHAR,
            F_804_1 VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO raw_register VALUES
            ('01234567', '1.234,5', '2.000,0', '120,0', '6,0', '3.000,5', '10,5', '4.000,0', '1.000,0', '12,0'),
            ('01234567', '100,5', '200,0', '30,0', '2,0', '300,5', '1,5', '400,0', '100,0', '2,0'),
            ('7654321', '50', '25', '5', '1', '10', '5', '40', '15', '1')
        """
    )

    loader = _loader(connection)
    loader._transform_gr_main_register("raw_register", "fertilizer_accounts", 2025, 2)

    rows = connection.execute(
        """
        SELECT cvr_number, year, tn_t_ha, mineral_n_foraar, mineral_n_eft,
               mineral_n_udb, organic_n_hus, mineral_n_total,
               mineral_n_autumn_total_kg, grazing_n_total_kg,
               harmoni_area_ha, total_n_consumption_kg,
               niveau, mineral_n_allocation_method
        FROM fertilizer_accounts
        ORDER BY cvr_number
        """
    ).fetchall()

    assert rows[0][:2] == ("01234567", 2025)
    assert rows[0][2:12] == pytest.approx(
        (0.11125, 0.0, 12.5, 2 / 3, 275.0833333333, 2200.0, 150.0, 8.0, 12.0, 1335.0)
    )
    assert rows[0][12:] == ("Detailed main register", "explicit_register_fields_only")
    assert rows[1][:2] == ("07654321", 2025)
    assert rows[1][2:12] == pytest.approx((0.01, 0.0, 1.0, 0.2, 2.0, 25.0, 5.0, 1.0, 5.0, 50.0))
    assert rows[1][12:] == ("Detailed main register", "explicit_register_fields_only")


def test_in_depth_file_selection_excludes_detail_tables() -> None:
    assert NLES5DataLoader._is_in_depth_main_register_file(
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_6A_harmonized.parquet"
    )
    assert NLES5DataLoader._is_in_depth_main_register_file(
        "bucket/silver/gr_2024/20260912_120000/V_4061GR_24_ISKV1_6B_pii_handled.parquet"
    )
    assert not NLES5DataLoader._is_in_depth_main_register_file(
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_B_DYRERK_harmonized.parquet"
    )
    assert not NLES5DataLoader._is_in_depth_main_register_file(
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_B_GOEDRK_6B.parquet"
    )


def test_file_selection_prefers_final_pii_copy() -> None:
    files = [
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_6A_harmonized.parquet",
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_6A_harmonized_schema.parquet",
        "bucket/silver/gr 2025/20260912_120000/V_4061GR_25_ISKV1_6A_harmonized_schema_pii_handled.parquet",
    ]

    assert NLES5DataLoader._prefer_final_gr_files(files) == [files[2]]


def test_gkea_loader_uses_storage_for_bare_bucket_paths(tmp_path) -> None:
    connection = duckdb.connect()
    source_path = tmp_path / "GKEA2024_fixture.parquet"
    connection.execute(
        """
        CREATE TABLE gkea_source AS
        SELECT '01234567'::VARCHAR AS cvr_number,
               'field-1'::VARCHAR AS marknummer,
               12.5::DOUBLE AS faktisk_areal_ha,
               'journal-1'::VARCHAR AS journal_nummer
        """
    )
    connection.execute(f"COPY gkea_source TO '{source_path}' (FORMAT PARQUET)")
    connection.execute("DROP TABLE gkea_source")

    cloud_path = "landbruget-data/silver/fertiliser/run/GKEA2024_Markplan.parquet"
    storage = _MappedGkeaStorage(connection, str(source_path))
    loader = _loader(connection, storage)

    assert loader._process_gkea_field_plan_data(cloud_path, "field_plan_data")
    assert storage.created_paths == [cloud_path]
    assert connection.execute(
        "SELECT cvr_number, marknummer, areal FROM field_plan_data"
    ).fetchall() == [("01234567", "field-1", 12.5)]


def test_gr_year_prefers_directory_and_supports_two_digit_filename() -> None:
    assert (
        NLES5DataLoader._extract_godningsregnskab_year(
            "silver/gr 2025/V_4061GR_25_ISKV1_6A.parquet", directory_year=2025
        )
        == 2025
    )
    assert (
        NLES5DataLoader._extract_godningsregnskab_year(
            "silver/fertiliser/V_4061GR_24_ISKV1_6A.parquet"
        )
        == 2024
    )


def test_loader_combines_multiple_years_and_all_parts(tmp_path) -> None:
    connection = duckdb.connect()
    storage = _Storage(connection)
    paths: dict[int, list[str]] = {}

    for year, cvr, total_n in ((2024, "01234567", "100,0"), (2025, "07654321", "200,0")):
        table = f"source_{year}"
        path = tmp_path / f"V_4061GR_{year % 100:02d}_ISKV1_6A.parquet"
        connection.execute(
            f"""
            CREATE TABLE {table} AS
            SELECT '{cvr}'::VARCHAR AS CVR,
                   '{total_n}'::VARCHAR AS F_901,
                   '10,0'::VARCHAR AS F_706_1,
                   '20,0'::VARCHAR AS F_308_1,
                   '10,0'::VARCHAR AS F_243
            """
        )
        connection.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
        connection.execute(f"DROP TABLE {table}")
        paths[year] = [str(path)]

    loader = _loader(connection, storage)
    loader._get_fertilizer_accounts_file_paths = lambda year: paths.get(year, [])

    assert loader._load_fertilizer_accounts_for_years([2024, 2025])
    assert connection.execute(
        "SELECT cvr_number, year, tn_t_ha FROM fertilizer_accounts ORDER BY year"
    ).fetchall() == [("01234567", 2024, 0.01), ("07654321", 2025, 0.02)]


def test_farm_loader_aggregates_detail_animal_rows_for_2025(tmp_path) -> None:
    connection = duckdb.connect()
    main_path = tmp_path / "V_4061GR_25_ISKV1_6A.parquet"
    animal_path = tmp_path / "V_4061GR_25_ISKV1_B_DYRERK.parquet"
    connection.execute(
        """
        CREATE TABLE main_source AS
        SELECT '01234567'::VARCHAR AS CVR, '100,0'::VARCHAR AS F_901
        """
    )
    connection.execute(f"COPY main_source TO '{main_path}' (FORMAT PARQUET)")
    connection.execute(
        """
        CREATE TABLE animal_source AS
        SELECT '01234567'::VARCHAR AS CVR,
               '100,0'::VARCHAR AS C_2016,
               '2'::VARCHAR AS C_2006,
               '3'::VARCHAR AS C_2017
        """
    )
    connection.execute(f"COPY animal_source TO '{animal_path}' (FORMAT PARQUET)")
    connection.execute("DROP TABLE main_source")
    connection.execute("DROP TABLE animal_source")

    storage = _Storage(connection, [str(main_path), str(animal_path)])
    loader = _loader(connection, storage)
    farm_table = loader._load_farm_data_for_year(2025)

    assert farm_table == "farm_data_2025"
    assert connection.execute(
        "SELECT cvr, organic_n_production, animal_count, animal_units FROM farm_data_2025"
    ).fetchone() == ("01234567", 100.0, 2.0, 3.0)


def test_fertilizer_distribution_keeps_reported_sources_separate() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE fertilizer_accounts (
            cvr_number VARCHAR,
            year INTEGER,
            organic_n_hus DOUBLE,
            mineral_n_foraar DOUBLE,
            mineral_n_eft DOUBLE,
            mineral_n_udb DOUBLE,
            tn_t_ha DOUBLE,
            harmoni_area_ha DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fertilizer_accounts VALUES
            ('01234567', 2025, 0.0, 3.0, 1.0, 0.5, 0.01, 10.0)
        """
    )
    connection.execute(
        """
        CREATE TABLE fields (
            field_id VARCHAR,
            cvr_number VARCHAR,
            year INTEGER,
            crop_name VARCHAR,
            m_code VARCHAR,
            area_ha DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fields VALUES
            ('field-1', '01234567', 2025, 'spring barley', 'M2', 5.0),
            ('field-2', '01234567', 2025, 'spring barley', 'M2', 5.0)
        """
    )

    distributor = NLES5FertilizerDistributor(connection, _Log())
    result_table = distributor.apply_fertilizer_distribution_to_pipeline("fields")

    rows = connection.execute(
        f"""
        SELECT mineral_n_foraar, mineral_n_eft, mineral_n_udb, tn_t_ha
        FROM {result_table}
        ORDER BY field_id
        """
    ).fetchall()

    assert rows == pytest.approx([(3.0, 1.0, 0.5, 0.01)] * 2)
