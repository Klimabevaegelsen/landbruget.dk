"""
Tests for CVR normalization utility.

CVR Format:
- Exactly 8 digits
- Stored as string (to preserve leading zeros)
"""

import pytest

from unified_pipeline.util.cvr_normalizer import (
    CVR_COLUMN_ALIASES,
    find_cvr_column,
    normalize_cvr,
    validate_cvr,
)


class TestNormalizeCVR:
    """Tests for the normalize_cvr function."""

    def test_valid_8_digit_string(self):
        """Test normalization of valid 8-digit string."""
        assert normalize_cvr("31373077") == "31373077"

    def test_valid_8_digit_int(self):
        """Test normalization of valid 8-digit integer."""
        assert normalize_cvr(31373077) == "31373077"

    def test_valid_float_conversion(self):
        """Test normalization of float (from CSV imports)."""
        assert normalize_cvr(31373077.0) == "31373077"
        assert normalize_cvr("31373077.0") == "31373077"

    def test_zero_padding_short_cvr(self):
        """Test zero-padding of CVRs shorter than 8 digits."""
        assert normalize_cvr("1234567") == "01234567"
        assert normalize_cvr("123456") == "00123456"
        assert normalize_cvr(1234567) == "01234567"

    def test_xml_hex_decoding(self):
        """Test XML hex encoding decoding (from FVM Excel exports)."""
        # _x0033_ = "3" (ASCII 51 = 0x33)
        assert normalize_cvr("_x0033_1373077") == "31373077"
        # _x0034_ = "4" (ASCII 52 = 0x34)
        assert normalize_cvr("_x0034_1373077") == "41373077"

    def test_none_input(self):
        """Test handling of None input."""
        assert normalize_cvr(None) is None

    def test_empty_string(self):
        """Test handling of empty string."""
        assert normalize_cvr("") is None
        assert normalize_cvr("   ") is None

    def test_too_many_digits(self):
        """Test rejection of CVRs with more than 8 digits."""
        assert normalize_cvr("123456789") is None  # 9 digits
        assert normalize_cvr("1234567890") is None  # 10 digits

    def test_non_numeric_input(self):
        """Test handling of non-numeric input."""
        assert normalize_cvr("abcdefgh") is None
        assert normalize_cvr("12-34-56-78") == "12345678"  # Extracts digits

    def test_leading_zeros_preserved(self):
        """Test that leading zeros are preserved/added."""
        assert normalize_cvr("01234567") == "01234567"
        assert normalize_cvr(1234567) == "01234567"


class TestValidateCVR:
    """Tests for the validate_cvr function."""

    def test_valid_cvr(self):
        """Test validation of valid 8-digit CVR."""
        assert validate_cvr("31373077") is True
        assert validate_cvr("01234567") is True
        assert validate_cvr("00000001") is True

    def test_invalid_length(self):
        """Test rejection of invalid length CVRs."""
        assert validate_cvr("1234567") is False  # 7 digits
        assert validate_cvr("123456789") is False  # 9 digits

    def test_invalid_characters(self):
        """Test rejection of CVRs with non-digit characters."""
        assert validate_cvr("1234567a") is False
        assert validate_cvr("12-34-56") is False

    def test_non_string_input(self):
        """Test rejection of non-string input."""
        assert validate_cvr(31373077) is False
        assert validate_cvr(None) is False


class TestFindCVRColumn:
    """Tests for the find_cvr_column function."""

    def test_exact_match(self):
        """Test exact column name match."""
        columns = ["id", "CVR", "name"]
        assert find_cvr_column(columns) == "CVR"

    def test_alias_match(self):
        """Test matching against known aliases."""
        # Test various aliases
        assert find_cvr_column(["id", "Ansoeger", "name"]) == "Ansoeger"
        assert find_cvr_column(["id", "KUNDE_LB", "name"]) == "KUNDE_LB"
        assert find_cvr_column(["id", "cvr_number", "name"]) == "cvr_number"

    def test_no_match(self):
        """Test when no CVR column is found."""
        columns = ["id", "name", "address"]
        assert find_cvr_column(columns) is None


class TestCVRColumnAliases:
    """Tests for the CVR_COLUMN_ALIASES mapping."""

    def test_aliases_exist(self):
        """Test that expected aliases are defined."""
        assert "CVR" in CVR_COLUMN_ALIASES
        assert "Ansoeger" in CVR_COLUMN_ALIASES
        assert "KUNDE_LB" in CVR_COLUMN_ALIASES

    def test_all_aliases_map_to_cvr(self):
        """Test that all aliases map to 'cvr'."""
        for alias, target in CVR_COLUMN_ALIASES.items():
            assert target == "cvr", f"Alias {alias} should map to 'cvr', got '{target}'"


class TestRealWorldCases:
    """Tests based on real data patterns observed in the codebase."""

    def test_stoetteoplysninger_cvr_format(self):
        """Test CVR format from støtteoplysninger (float from CSV)."""
        # støtteoplysninger has CVRs as floats in CSV
        assert normalize_cvr(88717813.0) == "88717813"
        assert normalize_cvr("88717813.0") == "88717813"

    def test_fvm_marker_cvr_format(self):
        """Test CVR format from FVM marker data."""
        # FVM data may have XML encoding
        assert normalize_cvr("12345678") == "12345678"
        assert normalize_cvr("_x0031_2345678") == "12345678"  # Encoded "1"

    def test_deminimis_cvr_format(self):
        """Test CVR format from de minimis data."""
        # De minimis may have short CVRs
        assert normalize_cvr("1234567") == "01234567"
