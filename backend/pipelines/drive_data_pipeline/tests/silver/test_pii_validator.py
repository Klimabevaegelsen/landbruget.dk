"""Regression tests for repeated PII handling on shared DuckDB connections."""

import duckdb
from drive_data_pipeline.silver.validators.pii_validator import PIIAction, PIIType, PIIValidator


def test_handle_pii_replaces_shared_output_table() -> None:
    """Repeated files can reuse the validator's fixed table names safely."""
    validator = PIIValidator(
        pii_types={PIIType.EMAIL},
        action=PIIAction.MASK,
        threshold=0.3,
    )
    validator.conn = duckdb.connect()

    validator.conn.execute("CREATE TABLE pii_validation_table (email VARCHAR, value INTEGER)")
    validator.conn.execute("INSERT INTO pii_validation_table VALUES ('first@example.com', 1)")
    first_result = validator.validate("pii_validation_table")
    first_table = validator.handle_pii("pii_validation_table", first_result)

    validator.conn.execute("DELETE FROM pii_validation_table")
    validator.conn.execute("INSERT INTO pii_validation_table VALUES ('second@example.com', 2)")
    second_result = validator.validate("pii_validation_table")
    second_table = validator.handle_pii("pii_validation_table", second_result)

    assert first_table == second_table == "pii_validation_table_pii_handled"
    assert validator.conn.execute(
        "SELECT email, value FROM pii_validation_table_pii_handled"
    ).fetchall() == [("***MASKED***", 2)]
