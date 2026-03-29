"""Tests for R2 filesystem utilities.

TDD RED phase: These tests define the expected behavior of the R2 filesystem
module BEFORE implementation. They should fail initially, then pass once
filesystem.py is migrated to s3fs.

Tests cover:
- S3FileSystem creation with R2 endpoint
- HMAC credential usage (R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
- Filesystem instance caching
- DuckDB TYPE r2 secret creation
- DuckDB fsspec registration with s3fs
- Graceful fallback when credentials are missing
"""

import os
from unittest.mock import MagicMock, patch

import duckdb
import pytest


class TestGetR2Filesystem:
    """Tests for get_r2_filesystem() function."""

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    @patch("common.storage.filesystem.s3fs.S3FileSystem")
    def test_creates_s3fs_with_r2_endpoint(self, mock_s3fs_class):
        """S3FileSystem should be created with R2-specific endpoint URL."""
        from common.storage.filesystem import get_r2_filesystem

        # Clear any cached instance
        get_r2_filesystem.cache_clear()

        mock_fs = MagicMock()
        mock_s3fs_class.return_value = mock_fs

        result = get_r2_filesystem()

        mock_s3fs_class.assert_called_once()
        call_kwargs = mock_s3fs_class.call_args
        # Verify endpoint URL follows R2 pattern
        endpoint = call_kwargs.kwargs.get("client_kwargs", {}).get("endpoint_url", "")
        assert "r2.cloudflarestorage.com" in endpoint
        assert "abc123def456ghi789jkl012mno345pqr" in endpoint
        assert result == mock_fs

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    @patch("common.storage.filesystem.s3fs.S3FileSystem")
    def test_uses_r2_credentials(self, mock_s3fs_class):
        """S3FileSystem should use R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY."""
        from common.storage.filesystem import get_r2_filesystem

        get_r2_filesystem.cache_clear()
        mock_s3fs_class.return_value = MagicMock()

        get_r2_filesystem()

        call_kwargs = mock_s3fs_class.call_args
        assert call_kwargs.kwargs.get("key") == "test-key-id"
        assert call_kwargs.kwargs.get("secret") == "test-secret-key"

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    @patch("common.storage.filesystem.s3fs.S3FileSystem")
    def test_caches_filesystem_instance(self, mock_s3fs_class):
        """Repeated calls should return the same cached filesystem instance."""
        from common.storage.filesystem import get_r2_filesystem

        get_r2_filesystem.cache_clear()
        mock_fs = MagicMock()
        mock_s3fs_class.return_value = mock_fs

        result1 = get_r2_filesystem()
        result2 = get_r2_filesystem()

        # Should only create one instance
        assert mock_s3fs_class.call_count == 1
        assert result1 is result2

    @patch.dict(os.environ, {}, clear=True)
    @patch("common.storage.filesystem.s3fs.S3FileSystem")
    def test_fallback_when_no_r2_credentials(self, mock_s3fs_class):
        """Should handle missing credentials gracefully."""
        from common.storage.filesystem import get_r2_filesystem

        get_r2_filesystem.cache_clear()

        # Should either raise an informative error or create anon filesystem
        # The exact behavior depends on implementation choice
        with pytest.raises((ValueError, EnvironmentError)):
            get_r2_filesystem()


class TestGetDuckDBWithR2:
    """Tests for get_duckdb_with_r2() function."""

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    def test_creates_type_r2_secret(self):
        """DuckDB should be configured with TYPE r2 secret containing ACCOUNT_ID."""
        from common.storage.filesystem import _setup_native_r2_auth

        conn = duckdb.connect(":memory:")
        try:
            result = _setup_native_r2_auth(conn)
            assert result is True

            # Verify the secret was created by listing secrets
            secrets = conn.execute("SELECT * FROM duckdb_secrets()").fetchall()
            # Should have at least one secret
            assert len(secrets) > 0
            # Check secret contains r2 type info
            secret_str = str(secrets)
            assert "r2" in secret_str.lower()
        finally:
            conn.close()

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    def test_duckdb_connection_returned(self):
        """get_duckdb_with_r2 should return a valid DuckDB connection."""
        from common.storage.filesystem import get_duckdb_with_r2

        get_duckdb_with_r2.cache_clear()

        conn = get_duckdb_with_r2()
        try:
            # Should be a valid connection that can execute queries
            result = conn.execute("SELECT 1 AS test").fetchone()
            assert result[0] == 1
        finally:
            get_duckdb_with_r2.cache_clear()

    @patch.dict(os.environ, {}, clear=True)
    def test_no_credentials_falls_back_to_fsspec(self):
        """Without R2 credentials, should try fsspec registration."""
        from common.storage.filesystem import _setup_native_r2_auth

        conn = duckdb.connect(":memory:")
        try:
            result = _setup_native_r2_auth(conn)
            # Without credentials, native auth should return False
            assert result is False
        finally:
            conn.close()

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "key-with-'quotes",
            "R2_SECRET_ACCESS_KEY": "secret-with-'quotes",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    def test_sql_escaping_in_credentials(self):
        """Credentials with special characters should be properly escaped."""
        from common.storage.filesystem import _setup_native_r2_auth

        conn = duckdb.connect(":memory:")
        try:
            # Should not raise SQL injection / syntax error
            result = _setup_native_r2_auth(conn)
            assert result is True
        finally:
            conn.close()

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key-id",
            "R2_SECRET_ACCESS_KEY": "test-secret-key",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    def test_setup_duckdb_cloud_auth_function_works(self):
        """New shared auth helper should configure native cloud auth."""
        from common.storage.filesystem import setup_duckdb_cloud_auth

        conn = duckdb.connect(":memory:")
        try:
            assert setup_duckdb_cloud_auth(conn) is True
        finally:
            conn.close()

    @patch.dict(
        os.environ,
        {
            "GCS_ACCESS_KEY_ID": "legacy-key",
            "GCS_SECRET_ACCESS_KEY": "legacy-secret",
            "HOME": os.environ.get("HOME", "/tmp"),  # noqa: S108
        },
        clear=True,
    )
    def test_setup_duckdb_cloud_auth_fallback(self):
        """Shared auth helper should keep legacy cloud HMAC fallback."""
        from common.storage.filesystem import setup_duckdb_cloud_auth

        conn = duckdb.connect(":memory:")
        try:
            assert setup_duckdb_cloud_auth(conn) is True
            secrets = conn.execute("SELECT * FROM duckdb_secrets()").fetchall()
            assert "gcs" in str(secrets).lower()
        finally:
            conn.close()
