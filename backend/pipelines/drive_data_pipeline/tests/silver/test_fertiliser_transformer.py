"""Regression tests for the in-depth GR Drive transformer."""

from drive_data_pipeline.silver.transformers.fertiliser_transformer import FertiliserTransformer


def test_csv_in_depth_main_register_preserves_form_codes(tmp_path) -> None:
    transformer = FertiliserTransformer()
    assert transformer.can_handle(tmp_path / "V_4061GR_24_ISKV1_6A.csv", {})
    assert transformer.can_handle(tmp_path / "DCKKON.V_4061GR_24_ISKV1_6A.csv", {})
    assert transformer.can_handle(tmp_path / "DCKKON.V_4061GR_25_ISKV1_B_DYRERK_6B.xls", {})
    assert transformer.can_handle(tmp_path / "V_COMPANYA.xls", {})
    assert transformer.can_handle(tmp_path / "B_AOGGOED_6B.xls", {})
    content = ("CVR;F_901;F_706_1;F_308_1\n01234567;1.234,5;2.000,0;3.000,5\n").encode("latin-1")
    source_path = tmp_path / "V_4061GR_24_ISKV1_6A.csv"
    source_path.write_bytes(content)

    result = transformer.transform(
        source_path,
        metadata=None,
        output_dir=tmp_path / "out",
    )

    assert result.success
    assert result.output_path is not None
    columns = {
        row[0]
        for row in transformer.conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{result.output_path}')"
        ).fetchall()
    }
    assert {"CVR", "F_901", "F_706_1", "F_308_1", "cvr_number"}.issubset(columns)
    assert transformer.conn.execute(
        f"SELECT CVR, F_901, source_year FROM read_parquet('{result.output_path}')"
    ).fetchone() == ("01234567", "1.234,5", 2024)


def test_detail_tables_are_preserved_for_downstream_animal_and_transfer_parsing() -> None:
    transformer = FertiliserTransformer()
    transformer.conn.execute(
        """
        CREATE TABLE raw_detail (
            CVR VARCHAR,
            C_155 VARCHAR,
            C_197 VARCHAR,
            C_2016 VARCHAR
        )
        """
    )
    transformer.conn.execute("INSERT INTO raw_detail VALUES ('01234567', '10,5', '2,0', '100,0')")

    output_table = transformer._process_in_depth_register(
        "raw_detail", "V_4061GR_25_ISKV1_B_DYRERK.xls"
    )
    columns = {row[0] for row in transformer.conn.execute(f"DESCRIBE {output_table}").fetchall()}

    assert {"CVR", "C_155", "C_197", "C_2016", "cvr_number"}.issubset(columns)
    assert transformer.conn.execute(
        f"SELECT cvr_number, source_year, C_2016 FROM {output_table}"
    ).fetchone() == ("01234567", 2025, "100,0")
