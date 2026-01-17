"""Test Danish identifier format validation and normalization.

This module tests the validation and formatting of Danish business identifiers:
- CVR (Centrale Virksomhedsregister): Company registration number (8 digits)
- CHR (Centrale Husdyrbrugsregister): Livestock herd ID (6 digits)
- BFE (Bygnings- og Boligregistret): Cadastral parcel number

These identifiers are the foundation of data joinability in the landbruget.dk
data model, and proper validation is critical for data quality.

Reference: .claude/rules/data-quality.md
"""

import re

import duckdb
import pandas as pd
import pytest

# =============================================================================
# CVR Number Tests (Company ID - 8 digits)
# =============================================================================


def test_cvr_format_validation_valid(cvr_validator: callable, valid_cvr_numbers: list[str]) -> None:
    """Test that valid CVR numbers pass validation."""
    for cvr in valid_cvr_numbers:
        assert cvr_validator(cvr), f"Valid CVR {cvr} should pass validation"


def test_cvr_format_validation_with_leading_zeros(cvr_validator: callable) -> None:
    """Test that CVR numbers with leading zeros are valid."""
    cvr_with_zeros = "00113115"
    assert cvr_validator(cvr_with_zeros), "CVR with leading zeros should be valid"
    assert len(cvr_with_zeros) == 8, "CVR must be exactly 8 digits"


def test_cvr_format_validation_invalid_length(cvr_validator: callable) -> None:
    """Test that CVR numbers with invalid length fail validation."""
    invalid_cvrs = [
        "1234567",  # 7 digits (too short)
        "123456789",  # 9 digits (too long)
        "123",  # 3 digits
        "",  # empty
    ]
    for cvr in invalid_cvrs:
        assert not cvr_validator(cvr), f"CVR {cvr} with invalid length should fail"


def test_cvr_format_validation_non_numeric(cvr_validator: callable) -> None:
    """Test that CVR numbers with non-numeric characters fail validation."""
    invalid_cvrs = [
        "1234567a",  # contains letter
        "12345-78",  # contains hyphen
        "12 345678",  # contains space
        "12.345678",  # contains period
    ]
    for cvr in invalid_cvrs:
        assert not cvr_validator(cvr), f"CVR {cvr} with non-numeric chars should fail"


def test_cvr_format_validation_wrong_type(cvr_validator: callable) -> None:
    """Test that non-string types fail CVR validation."""
    invalid_types = [
        12345678,  # integer
        12345678.0,  # float
        None,  # null
        [],  # list
    ]
    for value in invalid_types:
        assert not cvr_validator(value), f"CVR {value} of type {type(value)} should fail"


def test_cvr_formatting_preserves_leading_zeros(cvr_formatter: callable) -> None:
    """Test that CVR formatting preserves leading zeros."""
    test_cases = [
        (113115, "00113115"),
        ("113115", "00113115"),
        (123, "00000123"),
        ("00000123", "00000123"),
    ]
    for input_val, expected in test_cases:
        result = cvr_formatter(input_val)
        assert result == expected, f"CVR {input_val} should format to {expected}, got {result}"


def test_cvr_formatting_integer_input(cvr_formatter: callable) -> None:
    """Test CVR formatting with integer input."""
    cvr_int = 31373077
    result = cvr_formatter(cvr_int)
    assert result == "31373077", "Integer CVR should format to 8-digit string"
    assert isinstance(result, str), "Formatted CVR must be string type"


def test_cvr_formatting_invalid_raises_error(cvr_formatter: callable) -> None:
    """Test that invalid CVR numbers raise ValueError."""
    invalid_cvrs = [
        "1234567a",  # non-numeric (will still fail after zfill)
    ]
    for cvr in invalid_cvrs:
        with pytest.raises(ValueError, match="Invalid CVR format"):
            cvr_formatter(cvr)


# =============================================================================
# CHR Number Tests (Herd ID - 6 digits)
# =============================================================================


def test_chr_format_validation_valid(chr_validator: callable, valid_chr_numbers: list[str]) -> None:
    """Test that valid CHR numbers pass validation."""
    for chr_num in valid_chr_numbers:
        assert chr_validator(chr_num), f"Valid CHR {chr_num} should pass validation"


def test_chr_format_validation_invalid_length(chr_validator: callable) -> None:
    """Test that CHR numbers with invalid length fail validation."""
    invalid_chrs = [
        "12345",  # 5 digits (too short)
        "1234567",  # 7 digits (too long)
        "123",  # 3 digits
        "",  # empty
    ]
    for chr_num in invalid_chrs:
        assert not chr_validator(chr_num), f"CHR {chr_num} with invalid length should fail"


def test_chr_format_validation_non_numeric(chr_validator: callable) -> None:
    """Test that CHR numbers with non-numeric characters fail validation."""
    invalid_chrs = [
        "12345a",  # contains letter
        "123-456",  # contains hyphen
        "12 3456",  # contains space
    ]
    for chr_num in invalid_chrs:
        assert not chr_validator(chr_num), f"CHR {chr_num} with non-numeric chars should fail"


def test_chr_formatting_preserves_leading_zeros(chr_formatter: callable) -> None:
    """Test that CHR formatting preserves leading zeros."""
    test_cases = [
        (123, "000123"),
        ("123", "000123"),
        ("000123", "000123"),
        (1, "000001"),
    ]
    for input_val, expected in test_cases:
        result = chr_formatter(input_val)
        assert result == expected, f"CHR {input_val} should format to {expected}, got {result}"


def test_chr_formatting_integer_input(chr_formatter: callable) -> None:
    """Test CHR formatting with integer input."""
    chr_int = 123456
    result = chr_formatter(chr_int)
    assert result == "123456", "Integer CHR should format to 6-digit string"
    assert isinstance(result, str), "Formatted CHR must be string type"


def test_chr_formatting_invalid_raises_error(chr_formatter: callable) -> None:
    """Test that invalid CHR numbers raise ValueError."""
    invalid_chrs = [
        "12345a",  # non-numeric (will still fail after zfill)
    ]
    for chr_num in invalid_chrs:
        with pytest.raises(ValueError, match="Invalid CHR format"):
            chr_formatter(chr_num)


# =============================================================================
# BFE Number Tests (Cadastral ID - Variable Format)
# =============================================================================


def test_bfe_format_basic_pattern() -> None:
    """Test BFE number follows kommune-ejerlav-matr pattern."""
    valid_bfe_patterns = [
        "0101-123456-12a",
        "0851-234567-1",
        "0461-100000-123abc",
        "0101-000001-1a",
    ]
    # BFE format: kommune (4 digits) - ejerlav (6 digits) - matr (variable alphanumeric)
    bfe_pattern = r"^\d{4}-\d{6}-[0-9a-zA-Z]+$"

    for bfe in valid_bfe_patterns:
        assert re.match(bfe_pattern, bfe), f"BFE {bfe} should match pattern"


def test_bfe_format_invalid_pattern() -> None:
    """Test that invalid BFE patterns are rejected."""
    invalid_bfe_patterns = [
        "101-123456-12a",  # kommune too short
        "0101-12345-12a",  # ejerlav too short
        "0101-123456-",  # missing matr
        "0101123456-12a",  # missing first hyphen
        "0101-12345612a",  # missing second hyphen
        "",  # empty
    ]
    bfe_pattern = r"^\d{4}-\d{6}-[0-9a-zA-Z]+$"

    for bfe in invalid_bfe_patterns:
        assert not re.match(bfe_pattern, bfe), f"BFE {bfe} should not match pattern"


# =============================================================================
# Batch Validation Tests (DataFrame Operations)
# =============================================================================


def test_batch_cvr_validation_dataframe(mock_duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """Test batch CVR validation on DataFrame."""
    pd.DataFrame(
        {
            "cvr": ["31373077", "00113115", "12345678", "invalid", "1234567"],
            "company_name": ["Arla", "Test Co", "Generic", "Bad", "Short"],
        }
    )

    # Load into DuckDB
    mock_duckdb_connection.execute("CREATE TABLE companies AS SELECT * FROM df")

    # Validate CVR format using SQL
    result = mock_duckdb_connection.execute(
        """
        SELECT cvr, regexp_matches(cvr, '^[0-9]{8}$') as is_valid
        FROM companies
    """
    ).fetchdf()

    # Check validation results
    assert result.loc[0, "is_valid"]  # Valid CVR
    assert result.loc[1, "is_valid"]  # Valid with leading zeros
    assert result.loc[2, "is_valid"]  # Valid
    assert not result.loc[3, "is_valid"]  # Non-numeric
    assert not result.loc[4, "is_valid"]  # Too short


def test_batch_chr_validation_dataframe(mock_duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """Test batch CHR validation on DataFrame."""
    pd.DataFrame(
        {
            "chr": ["123456", "000123", "654321", "12345", "12345a"],
            "herd_type": ["Cattle", "Pig", "Cattle", "Invalid", "Invalid"],
        }
    )

    # Load into DuckDB
    mock_duckdb_connection.execute("CREATE TABLE herds AS SELECT * FROM df")

    # Validate CHR format using SQL
    result = mock_duckdb_connection.execute(
        """
        SELECT chr, regexp_matches(chr, '^[0-9]{6}$') as is_valid
        FROM herds
    """
    ).fetchdf()

    # Check validation results
    assert result.loc[0, "is_valid"]  # Valid CHR
    assert result.loc[1, "is_valid"]  # Valid with leading zeros
    assert result.loc[2, "is_valid"]  # Valid
    assert not result.loc[3, "is_valid"]  # Too short
    assert not result.loc[4, "is_valid"]  # Non-numeric


def test_identifier_normalization_pipeline(mock_duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """Test full identifier normalization pipeline (Bronze -> Silver)."""
    # Bronze: Raw data with inconsistent formatting
    pd.DataFrame(
        {
            "cvr": [31373077, 113115, "12345678", "00000123"],
            "chr": [123456, 123, "654321", "000001"],
            "bfe": ["0101-123456-12a", "0851-234567-1", "0461-100000-abc", "0101-000001-1a"],
        }
    )

    mock_duckdb_connection.execute("CREATE TABLE bronze_data AS SELECT * FROM raw_df")

    # Silver: Normalize identifiers
    silver_query = """
        SELECT
            LPAD(CAST(cvr AS VARCHAR), 8, '0') as cvr_normalized,
            LPAD(CAST(chr AS VARCHAR), 6, '0') as chr_normalized,
            bfe as bfe_normalized
        FROM bronze_data
    """

    result = mock_duckdb_connection.execute(silver_query).fetchdf()

    # Verify normalization
    assert all(result["cvr_normalized"].str.len() == 8), "All CVRs should be 8 digits"
    assert all(result["chr_normalized"].str.len() == 6), "All CHRs should be 6 digits"
    assert all(result["cvr_normalized"].str.match(r"^\d{8}$")), "All CVRs should be numeric"
    assert all(result["chr_normalized"].str.match(r"^\d{6}$")), "All CHRs should be numeric"

    # Check specific values
    assert result.loc[0, "cvr_normalized"] == "31373077"
    assert result.loc[1, "cvr_normalized"] == "00113115"
    assert result.loc[1, "chr_normalized"] == "000123"
