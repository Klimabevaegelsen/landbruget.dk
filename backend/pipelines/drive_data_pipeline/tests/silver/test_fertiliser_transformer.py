"""Regression tests for the in-depth GR Drive transformer."""

from drive_data_pipeline.silver.transformers.fertiliser_transformer import FertiliserTransformer


def _create_spatial_excel_table(transformer: FertiliserTransformer, table_name: str) -> None:
    """Create the table shape returned by DuckDB spatial ``st_read`` for these workbooks."""
    columns = ", ".join(['"OGC_FID" INTEGER'] + [f'"Field{i}" VARCHAR' for i in range(1, 22)])
    transformer.conn.execute(f"CREATE TABLE {table_name} ({columns})")


def test_gkea_spatial_excel_layout_maps_real_columns() -> None:
    transformer = FertiliserTransformer()
    _create_spatial_excel_table(transformer, "gkea_excel")
    transformer.conn.execute(
        """
        INSERT INTO gkea_excel VALUES
        (1, 'GKEA2024 Markplan', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        (2, NULL, NULL, NULL, NULL, 'C1', 'C2', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10',
            'C11', 'C12', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22'),
        (3, 'Journal Nummer', 'CVR', 'Modtaget Dato', 'Marknummer', 'Areal',
            'Harmoni Areal Indikator', 'Harmoni Areal', 'Jordbundstype', 'Jordbundstype Ændret',
            'Vanding Indikator', 'Hovedafgrøde', 'Forfrugt', 'Udlæg', 'N Fradrag Forfrugt',
            'N Norm Afgrøde', 'N Norm Udlæg', 'N Korrektion', 'Korrektion N Prognose',
            'N Kvote pr. Ha', 'N Kvote Mark', NULL),
        (4, '24-0037578', '44162180', '2024-05-07', '4-0', '3.81', 'Ja', '3.8', '2', NULL,
            NULL, '11', '11', NULL, '0', '178', '0', NULL, '0', '178', '676.4', NULL)
        """
    )

    output = transformer._process_gkea(
        "gkea_excel", "GKEA2024_Markplan_med_Gødningsoplysninger.xlsx"
    )

    assert transformer.conn.execute(
        f"""
        SELECT cvr_number, marknummer, faktisk_areal_ha, omregnet_areal_ha, journal_nummer
        FROM {output}
        WHERE faktisk_areal_ha IS NOT NULL
        """
    ).fetchall() == [("44162180", "4-0", 3.81, 3.8, "24-0037578")]


def test_efterafgroeder_spatial_excel_layout_maps_real_columns() -> None:
    transformer = FertiliserTransformer()
    _create_spatial_excel_table(transformer, "cover_crops_excel")
    transformer.conn.execute(
        """
        INSERT INTO cover_crops_excel VALUES
        (1, 'GKEA2024 Markplan Efterafgrøder', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        (2, NULL, NULL, NULL, NULL, 'C1', 'C10', 'C2', 'C14', 'C31', 'C33', 'C34', 'C35',
            'C36', 'C37', 'C38', 'C39', NULL, NULL, NULL, NULL, NULL),
        (3, 'Journal Nummer', 'CVR', 'Modtaget Dato', 'Marknummer', 'Hovedafgrøde', 'Areal',
            'Areal til rådighed for EA', 'Kystvand Opland', 'MR Frivillig Indikator',
            'Obligatoriske EA Indikator', 'Pligtige Og Husdyr EAIndikator', 'Så-tidspunkt',
            'EAEller Alternativ Type', 'Præcisions landbrug', 'Areal Omregnet Til EA', NULL,
            NULL, NULL, NULL, NULL, NULL),
        (4, '24-0037580', '44223627', '2024-09-09', '3-0', '11', '13.92', '13.92', '129',
            'Ja', NULL, NULL, NULL, 'Efterafgrøder', NULL, '13.92', NULL, NULL, NULL, NULL,
            NULL, NULL)
        """
    )

    output = transformer._process_efterafgroeder(
        "cover_crops_excel", "GKEA2024_Markplan_Efterafgrøder.xlsx"
    )

    assert transformer.conn.execute(
        f"""
        SELECT year, cvr_number, marknummer, faktisk_areal_ha, omregnet_areal_ha,
               indberet_alternativ
        FROM {output}
        WHERE faktisk_areal_ha IS NOT NULL
        """
    ).fetchall() == [("2024", "44223627", "3-0", 13.92, 13.92, "Efterafgrøder")]


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
