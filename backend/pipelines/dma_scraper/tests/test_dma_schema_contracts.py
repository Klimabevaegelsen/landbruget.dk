"""Schema & contract tests for DMA scraper pipeline.

Validates that the bronze output structure matches what silver expects,
and that silver output has the correct schema.

Note: Silver uses pyarrow + DuckDB. We test the contracts using pure
DuckDB to avoid the pyarrow dependency in the shared test environment.
"""

import json
from typing import ClassVar

import duckdb


class TestBronzeOutputSchema:
    """Bronze output must match the structure silver expects to UNNEST."""

    REQUIRED_COMPANY_FIELDS: ClassVar[set[str]] = {
        "miljoeaktoerUrl",  # Used as the `id` column in silver
        "navn",
        "cvr_number",
    }

    REQUIRED_NESTED_ARRAYS: ClassVar[set[str]] = {"Tilsyn", "Håndhævelser", "Afgørelser"}

    def test_record_has_company_identifier(self, sample_bronze_records):
        """Each record must have miljoeaktoerUrl — silver uses it as id."""
        for record in sample_bronze_records:
            assert "miljoeaktoerUrl" in record
            assert record["miljoeaktoerUrl"]  # not empty

    def test_record_has_nested_arrays(self, sample_bronze_records):
        """Each record must have the three nested arrays silver UNNESTs."""
        for record in sample_bronze_records:
            for array_key in self.REQUIRED_NESTED_ARRAYS:
                assert array_key in record, f"Missing {array_key}"
                assert isinstance(record[array_key], list)

    def test_nested_arrays_contain_dicts(self, sample_bronze_records):
        """Items in nested arrays must be dicts (silver calls to_json on them)."""
        record = sample_bronze_records[0]
        for item in record["Tilsyn"]:
            assert isinstance(item, dict)
        for item in record["Håndhævelser"]:
            assert isinstance(item, dict)
        for item in record["Afgørelser"]:
            assert isinstance(item, dict)

    def test_empty_nested_arrays_valid(self, sample_bronze_records):
        """Records with empty nested arrays are valid (company with no inspections)."""
        empty_record = sample_bronze_records[1]
        assert empty_record["Tilsyn"] == []
        assert empty_record["Håndhævelser"] == []
        assert empty_record["Afgørelser"] == []


class TestSilverOutputSchema:
    """Silver produces a flat table with id, section, detail_json columns."""

    EXPECTED_COLUMNS: ClassVar[set[str]] = {"id", "section", "detail_json"}
    VALID_SECTIONS: ClassVar[set[str]] = {"Tilsyn", "Haandhaevelser", "Afgørelser"}

    def _run_silver_sql(self, records):
        """Replicate silver's DuckDB SQL transform without pyarrow.

        Silver does: pa.Table.from_pylist(data) → DuckDB UNNEST → DataFrame.
        We replicate the SQL logic directly using DuckDB's JSON support.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL json; LOAD json")

        # Insert records as JSON and parse
        conn.execute("CREATE TABLE dma_raw_json (data JSON)")
        for rec in records:
            conn.execute("INSERT INTO dma_raw_json VALUES (?)", [json.dumps(rec)])

        # Replicate silver's UNNEST logic
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

        result = conn.execute("SELECT id, section, detail_json FROM dma_flattened").fetchall()
        columns = ["id", "section", "detail_json"]
        conn.close()
        return [dict(zip(columns, row, strict=False)) for row in result]

    def test_output_has_exactly_three_columns(self, sample_bronze_records):
        """Silver output must have exactly: id, section, detail_json."""
        rows = self._run_silver_sql(sample_bronze_records)
        if rows:
            assert set(rows[0].keys()) == self.EXPECTED_COLUMNS

    def test_output_sections_are_valid(self, sample_bronze_records):
        """Section column values must be one of the three expected types."""
        rows = self._run_silver_sql(sample_bronze_records)
        actual_sections = {r["section"] for r in rows}
        assert actual_sections.issubset(self.VALID_SECTIONS)

    def test_id_column_matches_miljoeaktoer_url(self, sample_bronze_records):
        """The id column must come from miljoeaktoerUrl in bronze."""
        rows = self._run_silver_sql(sample_bronze_records)
        expected_ids = {r["miljoeaktoerUrl"] for r in sample_bronze_records}
        actual_ids = {r["id"] for r in rows}
        assert actual_ids.issubset(expected_ids)

    def test_detail_json_is_string(self, sample_bronze_records):
        """detail_json column must be serialized JSON strings."""
        rows = self._run_silver_sql(sample_bronze_records)
        for row in rows:
            assert isinstance(row["detail_json"], str)

    def test_row_count_matches_total_nested_items(self, sample_bronze_records):
        """One row per nested array item across all three sections."""
        expected_count = sum(
            len(r.get("Tilsyn", [])) + len(r.get("Håndhævelser", [])) + len(r.get("Afgørelser", []))
            for r in sample_bronze_records
        )
        rows = self._run_silver_sql(sample_bronze_records)
        assert len(rows) == expected_count


class TestBronzeToSilverContract:
    """Verify the bronze → silver transformation contract."""

    def test_sanitization_removes_empty_keys(self, sample_bronze_record_with_empty_keys):
        """Silver sanitizes nested arrays — empty/blank keys are removed."""
        # Replicate silver's sanitization logic
        data = sample_bronze_record_with_empty_keys
        for rec in data:
            for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                items = rec.get(sec) or []
                rec[sec] = [{k: v for k, v in item.items() if k and k.strip()} for item in items]

        # Verify empty keys were removed
        for rec in data:
            for item in rec["Tilsyn"]:
                for key in item:
                    assert key.strip() != ""

    def test_records_with_no_nested_items_produce_no_rows(self):
        """A record with all empty arrays produces zero rows."""
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL json; LOAD json")

        record = {
            "miljoeaktoerUrl": "/empty",
            "Tilsyn": [],
            "Håndhævelser": [],
            "Afgørelser": [],
        }
        conn.execute("CREATE TABLE dma_raw_json (data JSON)")
        conn.execute("INSERT INTO dma_raw_json VALUES (?)", [json.dumps(record)])

        # UNNEST of empty arrays should produce zero rows
        count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT 1 FROM dma_raw_json, UNNEST(from_json(data->'Tilsyn', '["JSON"]')) AS t(detail)
                UNION ALL
                SELECT 1 FROM dma_raw_json, UNNEST(from_json(data->'Håndhævelser', '["JSON"]')) AS t(detail)
                UNION ALL
                SELECT 1 FROM dma_raw_json, UNNEST(from_json(data->'Afgørelser', '["JSON"]')) AS t(detail)
            )
        """).fetchone()[0]
        conn.close()
        assert count == 0

    def test_duckdb_can_unnest_nested_arrays(self, sample_bronze_records):
        """DuckDB must be able to UNNEST the nested arrays from bronze JSON."""
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL json; LOAD json")
        conn.execute("CREATE TABLE dma_raw_json (data JSON)")

        for rec in sample_bronze_records:
            conn.execute("INSERT INTO dma_raw_json VALUES (?)", [json.dumps(rec)])

        # This should not throw — validates the UNNEST contract
        result = conn.execute("""
            SELECT data->>'miljoeaktoerUrl' AS id, to_json(detail) AS detail_json
            FROM dma_raw_json, UNNEST(from_json(data->'Tilsyn', '["JSON"]')) AS t(detail)
        """).fetchall()
        conn.close()

        assert len(result) > 0
        assert result[0][0] is not None  # id not null
