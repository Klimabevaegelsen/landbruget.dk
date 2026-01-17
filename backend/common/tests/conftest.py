"""Shared pytest fixtures for backend testing.

This module provides common fixtures used across all backend pipeline tests,
including mocks for GCS, DuckDB, and Danish data validation helpers.
"""

import tempfile
from collections.abc import Generator
from datetime import date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import duckdb
import pytest


@pytest.fixture(scope="function")
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test data.

    Scope: function - Each test gets its own clean temporary directory.

    Yields:
        Path: Path object pointing to temporary directory.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="function")
def mock_gcs_filesystem() -> Generator[MagicMock, None, None]:
    """Mock gcsfs.GCSFileSystem for isolated testing.

    Provides a mock GCS filesystem that simulates file operations without
    actual cloud storage interaction. Useful for testing upload/download logic.

    Scope: function - Each test gets its own mock to avoid state pollution.

    Yields:
        MagicMock: Mocked GCSFileSystem with common methods configured.
    """
    mock_fs = MagicMock()

    # Mock common GCS operations
    mock_fs.ls.return_value = []
    mock_fs.exists.return_value = False
    mock_fs.info.return_value = {"size": 0, "type": "file"}

    # Mock file operations
    mock_fs.open.return_value.__enter__ = Mock()
    mock_fs.open.return_value.__exit__ = Mock(return_value=False)

    # Track uploaded files for assertions
    mock_fs.uploaded_files = {}

    def mock_upload(local_path: str, remote_path: str) -> None:
        """Track uploaded files."""
        mock_fs.uploaded_files[remote_path] = local_path

    mock_fs.put.side_effect = mock_upload

    yield mock_fs


@pytest.fixture(scope="function")
def mock_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """In-memory DuckDB connection with spatial extension loaded.

    Provides a fresh DuckDB connection for each test with the spatial
    extension (h3, ST functions) pre-loaded.

    Scope: function - Each test gets its own isolated database.

    Yields:
        DuckDBPyConnection: In-memory DuckDB connection.
    """
    conn = duckdb.connect(":memory:")

    # Install and load spatial extension
    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
    except Exception:
        # Spatial extension might already be loaded or not available
        pass

    yield conn

    # Cleanup
    conn.close()


@pytest.fixture(scope="session")
def sample_danish_geometries() -> dict[str, dict[str, str]]:
    """Valid WKT geometries in EPSG:25832 and EPSG:4326.

    Provides sample geometries representing Danish locations in both the
    Danish coordinate system (EPSG:25832) and WGS84 (EPSG:4326).

    Scope: session - Geometries are immutable and can be shared.

    Returns:
        Dictionary mapping location names to geometry dictionaries with
        'epsg_25832' and 'epsg_4326' WKT strings.
    """
    return {
        "copenhagen_point": {
            "epsg_25832": "POINT(723626.13 6176067.15)",  # UTM Zone 32N
            "epsg_4326": "POINT(12.5683 55.6761)",  # WGS84 (lon, lat)
            "name": "Copenhagen City Hall",
        },
        "aarhus_point": {
            "epsg_25832": "POINT(577030.82 6224098.93)",  # UTM Zone 32N
            "epsg_4326": "POINT(10.2039 56.1629)",  # WGS84 (lon, lat)
            "name": "Aarhus City Hall",
        },
        "denmark_bbox": {
            "epsg_4326": "POLYGON((7.5 54.5, 15.5 54.5, 15.5 58, 7.5 58, 7.5 54.5))",
            "name": "Denmark bounding box",
        },
        "sample_field_polygon": {
            "epsg_25832": "POLYGON((723000 6176000, 724000 6176000, 724000 6177000, 723000 6177000, 723000 6176000))",
            "epsg_4326": "POLYGON((12.56 55.67, 12.58 55.67, 12.58 55.68, 12.56 55.68, 12.56 55.67))",
            "name": "Sample agricultural field near Copenhagen",
        },
    }


@pytest.fixture(scope="session")
def valid_cvr_numbers() -> list[str]:
    """Sample valid 8-digit CVR numbers with leading zeros.

    CVR (Centrale Virksomhedsregister) is the Danish company registration number.
    Format: Exactly 8 digits, stored as string to preserve leading zeros.

    Scope: session - CVR numbers are immutable and can be shared.

    Returns:
        List of valid CVR number strings.
    """
    return [
        "31373077",  # Arla Foods
        "10150817",  # Danish Crown
        "00113115",  # Example with leading zeros
        "12345678",  # Generic test CVR
        "87654321",  # Generic test CVR
    ]


@pytest.fixture(scope="session")
def valid_chr_numbers() -> list[str]:
    """Sample valid 6-digit CHR numbers.

    CHR (Centrale Husdyrbrugsregister) is the Danish livestock herd ID.
    Format: Exactly 6 digits, stored as string.

    Scope: session - CHR numbers are immutable and can be shared.

    Returns:
        List of valid CHR number strings.
    """
    return [
        "123456",
        "654321",
        "111111",
        "999999",
        "500001",
    ]


@pytest.fixture(scope="function")
def sample_date_range() -> dict[str, date]:
    """Sample date range for testing time-based queries.

    Scope: function - Allows tests to modify if needed.

    Returns:
        Dictionary with 'start' and 'end' date objects.
    """
    return {
        "start": date(2024, 1, 1),
        "end": date(2024, 12, 31),
    }


@pytest.fixture(scope="function")
def mock_supabase_client() -> Generator[MagicMock, None, None]:
    """Mock Supabase client for isolated testing.

    Provides a mock Supabase client that simulates database operations without
    actual database interaction.

    Scope: function - Each test gets its own mock to avoid state pollution.

    Yields:
        MagicMock: Mocked Supabase client with common methods configured.
    """
    mock_client = MagicMock()

    # Mock table operations
    mock_table = MagicMock()
    mock_client.table.return_value = mock_table

    # Mock query builder pattern
    mock_table.select.return_value = mock_table
    mock_table.insert.return_value = mock_table
    mock_table.upsert.return_value = mock_table
    mock_table.update.return_value = mock_table
    mock_table.delete.return_value = mock_table
    mock_table.eq.return_value = mock_table
    mock_table.neq.return_value = mock_table
    mock_table.gt.return_value = mock_table
    mock_table.gte.return_value = mock_table
    mock_table.lt.return_value = mock_table
    mock_table.lte.return_value = mock_table
    mock_table.like.return_value = mock_table
    mock_table.ilike.return_value = mock_table
    mock_table.is_.return_value = mock_table
    mock_table.in_.return_value = mock_table
    mock_table.order.return_value = mock_table
    mock_table.limit.return_value = mock_table

    # Mock execute() to return empty result
    mock_table.execute.return_value = MagicMock(data=[], count=0)

    yield mock_client


def validate_cvr_format(cvr: str) -> bool:
    """Validate CVR number format.

    Args:
        cvr: CVR number string to validate.

    Returns:
        True if valid 8-digit format, False otherwise.
    """
    if not isinstance(cvr, str):
        return False
    return len(cvr) == 8 and cvr.isdigit()


def validate_chr_format(chr_num: str) -> bool:
    """Validate CHR number format.

    Args:
        chr_num: CHR number string to validate.

    Returns:
        True if valid 6-digit format, False otherwise.
    """
    if not isinstance(chr_num, str):
        return False
    return len(chr_num) == 6 and chr_num.isdigit()


def format_cvr(cvr: int | str) -> str:
    """Format CVR number with leading zeros.

    Args:
        cvr: CVR number as int or string.

    Returns:
        8-digit CVR string with leading zeros.

    Raises:
        ValueError: If CVR is invalid format.
    """
    cvr_str = str(cvr).zfill(8)
    if not validate_cvr_format(cvr_str):
        msg = f"Invalid CVR format: {cvr}"
        raise ValueError(msg)
    return cvr_str


def format_chr(chr_num: int | str) -> str:
    """Format CHR number with leading zeros.

    Args:
        chr_num: CHR number as int or string.

    Returns:
        6-digit CHR string with leading zeros.

    Raises:
        ValueError: If CHR is invalid format.
    """
    chr_str = str(chr_num).zfill(6)
    if not validate_chr_format(chr_str):
        msg = f"Invalid CHR format: {chr_num}"
        raise ValueError(msg)
    return chr_str


# Make validation functions available to tests
@pytest.fixture(scope="session")
def cvr_validator() -> callable:
    """Provide CVR validation function to tests.

    Returns:
        Callable that validates CVR format.
    """
    return validate_cvr_format


@pytest.fixture(scope="session")
def chr_validator() -> callable:
    """Provide CHR validation function to tests.

    Returns:
        Callable that validates CHR format.
    """
    return validate_chr_format


@pytest.fixture(scope="session")
def cvr_formatter() -> callable:
    """Provide CVR formatting function to tests.

    Returns:
        Callable that formats CVR with leading zeros.
    """
    return format_cvr


@pytest.fixture(scope="session")
def chr_formatter() -> callable:
    """Provide CHR formatting function to tests.

    Returns:
        Callable that formats CHR with leading zeros.
    """
    return format_chr
