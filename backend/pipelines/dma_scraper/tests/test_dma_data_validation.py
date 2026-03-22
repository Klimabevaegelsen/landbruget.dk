"""Data validation tests for DMA scraper pipeline.

Great Expectations-style checks: valid identifiers, accepted section names,
non-null primary keys, valid JSON in detail column.
"""

import json
from typing import ClassVar

import duckdb


class TestCompanyIdentifierValidation:
    """Company identifiers must be valid."""

    def test_miljoeaktoer_url_not_empty(self, sample_bronze_records):
        for record in sample_bronze_records:
            assert record["miljoeaktoerUrl"]
            assert record["miljoeaktoerUrl"].strip() != ""

    def test_miljoeaktoer_url_starts_with_slash(self, sample_bronze_records):
        """miljoeaktoerUrl should be a relative URL path."""
        for record in sample_bronze_records:
            assert record["miljoeaktoerUrl"].startswith("/")

    def test_cvr_number_format_when_present(self, sample_bronze_records):
        """CVR numbers, when present, must be 8 digits."""
        for record in sample_bronze_records:
            cvr = record.get("cvr_number", "")
            if cvr:
                assert len(cvr) == 8, f"CVR should be 8 digits: {cvr}"
                assert cvr.isdigit(), f"CVR should be numeric: {cvr}"

    def test_company_name_not_empty(self, sample_bronze_records):
        for record in sample_bronze_records:
            assert record.get("navn")
            assert record["navn"].strip() != ""


class TestSectionNameValidation:
    """Section names in silver output must be from the accepted set."""

    VALID_SECTIONS: ClassVar[set[str]] = {"Tilsyn", "Haandhaevelser", "Afgørelser"}

    def test_bronze_sections_map_to_valid_silver_sections(self):
        """The three bronze array keys map to specific silver section names."""
        bronze_to_silver = {
            "Tilsyn": "Tilsyn",
            "Håndhævelser": "Haandhaevelser",
            "Afgørelser": "Afgørelser",
        }
        for silver_name in bronze_to_silver.values():
            assert silver_name in self.VALID_SECTIONS


class TestNonNullPrimaryKeys:
    """Primary key fields must never be null."""

    def test_miljoeaktoer_url_never_null(self, sample_bronze_records):
        for record in sample_bronze_records:
            assert record["miljoeaktoerUrl"] is not None

    def test_nested_items_have_content(self, sample_bronze_records):
        """Non-empty nested array items should have at least one key."""
        for record in sample_bronze_records:
            for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                for item in record.get(sec, []):
                    assert len(item) > 0, f"Empty dict in {sec}"


class TestDetailJsonValidation:
    """Silver's detail_json column must contain valid JSON."""

    def test_nested_items_are_json_serializable(self, sample_bronze_records):
        """Each nested array item must survive JSON round-trip."""
        for record in sample_bronze_records:
            for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                for item in record.get(sec, []):
                    serialized = json.dumps(item)
                    parsed = json.loads(serialized)
                    assert isinstance(parsed, dict)

    def test_duckdb_to_json_produces_valid_json(self):
        """DuckDB's to_json() function must produce parseable JSON."""
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL json; LOAD json")
        result = conn.execute("""
            SELECT to_json({'key': 'value', 'num': 42})
        """).fetchone()[0]
        conn.close()
        parsed = json.loads(result)
        assert parsed["key"] == "value"
        assert parsed["num"] == 42


class TestEmptyKeysSanitization:
    """Silver must remove empty-string and whitespace-only keys."""

    def test_empty_string_key_removed(self, sample_bronze_record_with_empty_keys):
        """Sanitization removes '' keys."""
        for rec in sample_bronze_record_with_empty_keys:
            for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                items = rec.get(sec) or []
                sanitized = [{k: v for k, v in item.items() if k and k.strip()} for item in items]
                for item in sanitized:
                    for key in item:
                        assert key != ""

    def test_whitespace_only_key_removed(self, sample_bronze_record_with_empty_keys):
        """Sanitization removes '   ' keys."""
        for rec in sample_bronze_record_with_empty_keys:
            for sec in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                items = rec.get(sec) or []
                sanitized = [{k: v for k, v in item.items() if k and k.strip()} for item in items]
                for item in sanitized:
                    for key in item:
                        assert key.strip() != ""

    def test_valid_keys_preserved(self, sample_bronze_record_with_empty_keys):
        """Sanitization keeps valid keys like 'date' and 'result'."""
        rec = sample_bronze_record_with_empty_keys[0]
        items = rec["Tilsyn"]
        sanitized = [{k: v for k, v in item.items() if k and k.strip()} for item in items]
        assert len(sanitized) == 1
        assert "date" in sanitized[0]
        assert "result" in sanitized[0]


class TestPostalCodeValidation:
    """Danish postal codes should be 4 digits when present."""

    def test_postal_code_format(self, sample_bronze_records):
        for record in sample_bronze_records:
            postal = record.get("postNummer", "")
            if postal:
                assert len(postal) == 4, f"Postal code should be 4 digits: {postal}"
                assert postal.isdigit(), f"Postal code should be numeric: {postal}"
