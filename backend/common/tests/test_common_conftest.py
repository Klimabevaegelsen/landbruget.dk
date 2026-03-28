"""Tests for common test fixtures.

This test file validates that the shared fixtures in conftest.py work correctly.
"""

from pathlib import Path

import duckdb
import pytest


def test_temp_dir_fixture(temp_dir):
    """Test that temp_dir fixture creates a valid temporary directory."""
    assert isinstance(temp_dir, Path)
    assert temp_dir.exists()
    assert temp_dir.is_dir()

    # Should be writable
    test_file = temp_dir / "test.txt"
    test_file.write_text("test content")
    assert test_file.exists()
    assert test_file.read_text() == "test content"


def test_mock_cloud_filesystem(mock_cloud_filesystem):
    """Test that mock_cloud_filesystem provides expected interface."""
    # Should have common cloud storage methods
    assert hasattr(mock_cloud_filesystem, "ls")
    assert hasattr(mock_cloud_filesystem, "exists")
    assert hasattr(mock_cloud_filesystem, "open")
    assert hasattr(mock_cloud_filesystem, "put")

    # Should track uploaded files
    assert hasattr(mock_cloud_filesystem, "uploaded_files")
    assert isinstance(mock_cloud_filesystem.uploaded_files, dict)


def test_mock_duckdb_connection(mock_duckdb_connection):
    """Test that mock_duckdb_connection works with basic queries."""
    conn = mock_duckdb_connection

    # Should be a valid DuckDB connection
    assert isinstance(conn, duckdb.DuckDBPyConnection)

    # Should support basic SQL
    result = conn.execute("SELECT 1 as test_col").fetchone()
    assert result[0] == 1

    # Should support table creation
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'test')")

    result = conn.execute("SELECT * FROM test_table").fetchone()
    assert result[0] == 1
    assert result[1] == "test"


def test_sample_danish_geometries(sample_danish_geometries):
    """Test that sample_danish_geometries provides valid data."""
    assert isinstance(sample_danish_geometries, dict)

    # Should have key locations
    assert "copenhagen_point" in sample_danish_geometries
    assert "aarhus_point" in sample_danish_geometries
    assert "denmark_bbox" in sample_danish_geometries

    # Each geometry should have required fields
    cph = sample_danish_geometries["copenhagen_point"]
    assert "epsg_4326" in cph
    assert "name" in cph
    assert "POINT" in cph["epsg_4326"]


def test_valid_cvr_numbers(valid_cvr_numbers):
    """Test that valid_cvr_numbers contains valid CVR formats."""
    assert isinstance(valid_cvr_numbers, list)
    assert len(valid_cvr_numbers) > 0

    for cvr in valid_cvr_numbers:
        # Should be 8-digit string
        assert isinstance(cvr, str)
        assert len(cvr) == 8
        assert cvr.isdigit()


def test_valid_chr_numbers(valid_chr_numbers):
    """Test that valid_chr_numbers contains valid CHR formats."""
    assert isinstance(valid_chr_numbers, list)
    assert len(valid_chr_numbers) > 0

    for chr_num in valid_chr_numbers:
        # Should be 6-digit string
        assert isinstance(chr_num, str)
        assert len(chr_num) == 6
        assert chr_num.isdigit()


def test_cvr_validator(cvr_validator, valid_cvr_numbers):
    """Test CVR validator function."""
    # Valid CVRs should pass
    for cvr in valid_cvr_numbers:
        assert cvr_validator(cvr)

    # Invalid CVRs should fail
    assert not cvr_validator("123")  # Too short
    assert not cvr_validator("123456789")  # Too long
    assert not cvr_validator("1234567a")  # Non-digit
    assert not cvr_validator(12345678)  # Not a string


def test_chr_validator(chr_validator, valid_chr_numbers):
    """Test CHR validator function."""
    # Valid CHRs should pass
    for chr_num in valid_chr_numbers:
        assert chr_validator(chr_num)

    # Invalid CHRs should fail
    assert not chr_validator("123")  # Too short
    assert not chr_validator("1234567")  # Too long
    assert not chr_validator("12345a")  # Non-digit
    assert not chr_validator(123456)  # Not a string


def test_cvr_formatter(cvr_formatter):
    """Test CVR formatter function."""
    # Should add leading zeros
    assert cvr_formatter("123") == "00000123"
    assert cvr_formatter(123) == "00000123"
    assert cvr_formatter("12345678") == "12345678"

    # Should raise on invalid input
    with pytest.raises(ValueError):
        cvr_formatter("123456789")  # Too long


def test_chr_formatter(chr_formatter):
    """Test CHR formatter function."""
    # Should add leading zeros
    assert chr_formatter("123") == "000123"
    assert chr_formatter(123) == "000123"
    assert chr_formatter("123456") == "123456"

    # Should raise on invalid input
    with pytest.raises(ValueError):
        chr_formatter("1234567")  # Too long


def test_sample_date_range(sample_date_range):
    """Test that sample_date_range provides valid dates."""
    assert isinstance(sample_date_range, dict)
    assert "start" in sample_date_range
    assert "end" in sample_date_range

    # End should be after start
    assert sample_date_range["end"] >= sample_date_range["start"]


def test_mock_supabase_client(mock_supabase_client):
    """Test that mock_supabase_client provides expected interface."""
    # Should have table method
    assert hasattr(mock_supabase_client, "table")

    # Table should support query builder pattern
    table = mock_supabase_client.table("test_table")
    assert hasattr(table, "select")
    assert hasattr(table, "insert")
    assert hasattr(table, "upsert")
    assert hasattr(table, "delete")

    # Should chain methods
    query = table.select("*").eq("id", 1).limit(10)
    assert query is not None
