"""Silver transform tests for DMA scraper pipeline.

Tests the DuckDB SQL flattening that converts nested bronze JSON
into the flat (id, section, detail_json) silver schema.
"""

import json

import duckdb

from silver.transformation import transform_dma_json


def _transform_with_duckdb(records):
    """Replicate silver's transform_dma_json using pure DuckDB.

    Silver uses pyarrow → DuckDB. We skip pyarrow and use DuckDB JSON
    directly to test the SQL logic.
    """
    # Apply silver's sanitization first
    for rec in records:
        for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
            items = rec.get(sec) or []
            rec[sec] = [{k: v for k, v in item.items() if k and k.strip()} for item in items]

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL json; LOAD json")
    conn.execute("CREATE TABLE dma_raw_json (data JSON)")

    for rec in records:
        conn.execute("INSERT INTO dma_raw_json VALUES (?)", [json.dumps(rec)])

    conn.execute("""
        CREATE TABLE dma_flattened AS
        SELECT data->>'miljoeaktoerUrl' AS id,
               'Tilsyn' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw_json, UNNEST(from_json(data->'Tilsyn', '["JSON"]')) AS t(detail)
        UNION ALL
        SELECT data->>'miljoeaktoerUrl' AS id,
               'Haandhaevelser' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw_json, UNNEST(from_json(data->'Håndhævelser', '["JSON"]')) AS t(detail)
        UNION ALL
        SELECT data->>'miljoeaktoerUrl' AS id,
               'Afgørelser' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw_json, UNNEST(from_json(data->'Afgørelser', '["JSON"]')) AS t(detail)
    """)

    rows = conn.execute("SELECT id, section, detail_json FROM dma_flattened").fetchall()
    conn.close()
    return rows


class TestTransformFlattening:
    """Test the UNNEST-based flattening of nested arrays."""

    def test_real_transform_returns_dataframe(self, sample_bronze_records):
        """The real pyarrow + DuckDB transform works with the pinned DuckDB API."""
        df = transform_dma_json(sample_bronze_records)
        assert list(df.columns) == ["id", "section", "detail_json"]
        assert len(df) == 3

    def test_produces_correct_row_count(self, sample_bronze_records):
        """Output rows = sum of all nested array items."""
        expected = sum(
            len(r.get("Tilsyn", [])) + len(r.get("Håndhævelser", [])) + len(r.get("Afgørelser", []))
            for r in sample_bronze_records
        )
        rows = _transform_with_duckdb(sample_bronze_records)
        assert len(rows) == expected

    def test_tilsyn_rows_have_correct_section(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        tilsyn_rows = [r for r in rows if r[1] == "Tilsyn"]
        # First record has 1 Tilsyn item
        assert len(tilsyn_rows) == 1

    def test_haandhaevelser_rows_created(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        haand_rows = [r for r in rows if r[1] == "Haandhaevelser"]
        assert len(haand_rows) == 1

    def test_afgoerelser_rows_created(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        afg_rows = [r for r in rows if r[1] == "Afgørelser"]
        assert len(afg_rows) == 1

    def test_id_comes_from_miljoeaktoer_url(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        ids = {r[0] for r in rows}
        expected_ids = {rec["miljoeaktoerUrl"] for rec in sample_bronze_records}
        assert ids.issubset(expected_ids)


class TestTransformDetailJson:
    """Test the detail_json column content."""

    def test_detail_json_is_valid_json(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        for _id, _section, detail in rows:
            parsed = json.loads(detail)
            assert isinstance(parsed, dict)

    def test_tilsyn_detail_preserves_fields(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        tilsyn_row = next(r for r in rows if r[1] == "Tilsyn")
        detail = json.loads(tilsyn_row[2])
        assert "date" in detail
        assert "result" in detail

    def test_haandhaevelse_detail_preserves_fields(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        haand_row = next(r for r in rows if r[1] == "Haandhaevelser")
        detail = json.loads(haand_row[2])
        assert "action" in detail

    def test_afgorelse_detail_preserves_fields(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        afg_row = next(r for r in rows if r[1] == "Afgørelser")
        detail = json.loads(afg_row[2])
        assert "decision" in detail


class TestTransformEmptyHandling:
    """Test handling of empty and missing data."""

    def test_empty_arrays_produce_no_rows(self):
        records = [
            {
                "miljoeaktoerUrl": "/empty",
                "Tilsyn": [],
                "Håndhævelser": [],
                "Afgørelser": [],
            }
        ]
        rows = _transform_with_duckdb(records)
        assert len(rows) == 0

    def test_missing_array_keys_handled(self):
        """Records missing nested arrays should be treated as empty."""
        records = [
            {
                "miljoeaktoerUrl": "/partial",
                "Tilsyn": [{"date": "2025-01-01"}],
            }
        ]
        # Add missing keys (silver's sanitization expects them)
        records[0].setdefault("Håndhævelser", [])
        records[0].setdefault("Afgørelser", [])
        rows = _transform_with_duckdb(records)
        assert len(rows) == 1
        assert rows[0][1] == "Tilsyn"

    def test_sanitization_removes_empty_keys(self, sample_bronze_record_with_empty_keys):
        rows = _transform_with_duckdb(sample_bronze_record_with_empty_keys)
        assert len(rows) == 1
        detail = json.loads(rows[0][2])
        for key in detail:
            assert key.strip() != ""


class TestTransformMultipleRecords:
    """Test handling of multiple records with varying nested data."""

    def test_multiple_companies_flattened(self, sample_bronze_records):
        """Two companies: one with data, one empty — only non-empty produces rows."""
        rows = _transform_with_duckdb(sample_bronze_records)
        ids = {r[0] for r in rows}
        # Only the first record has nested items
        assert "/Miljoeaktoer/100001" in ids
        assert "/Miljoeaktoer/100002" not in ids

    def test_all_sections_represented(self, sample_bronze_records):
        rows = _transform_with_duckdb(sample_bronze_records)
        sections = {r[1] for r in rows}
        assert sections == {"Tilsyn", "Haandhaevelser", "Afgørelser"}
