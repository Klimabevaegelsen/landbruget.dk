"""Tests for R2 core data access layer.

TDD RED phase: Tests define expected behavior of StorageAccess after R2 migration.
The class keeps its name (StorageAccess) for backward compatibility but internally
uses s3fs and R2 paths.

Tests cover:
- Path normalization (r2://, s3://, bare bucket paths)
- DuckDB filesystem registration with s3fs/R2
- Native R2 secret detection
- Upload/download JSON with s3fs mock
- Parquet operations via DuckDB with r2:// paths
- Danish character roundtrip (æøå)
"""

import io
import os
from unittest.mock import MagicMock, patch

import duckdb

# Ensure tenacity is available (installed as a real dependency)
# No mocking needed - tenacity works fine in tests, the @retry decorator
# just passes through when no retries are needed


class TestPathNormalization:
    """Test that paths are normalized correctly for R2/s3fs."""

    def _make_storage_access(self, mock_fs):
        """Create a StorageAccess with mocked filesystem."""
        with (
            patch("common.storage.filesystem.get_r2_filesystem", return_value=mock_fs),
            patch("common.storage.core.get_r2_filesystem", return_value=mock_fs),
            patch("common.storage.core.ResourceMonitor"),
        ):
            from common.storage.core import StorageAccess

            storage = StorageAccess.__new__(StorageAccess)
            storage.fs = mock_fs
            storage.log = MagicMock()
            storage.duckdb_conn = duckdb.connect(":memory:")
            storage.monitor = MagicMock()
            storage._native_cloud_available = False
            return storage

    def test_file_exists_with_bare_path(self):
        """file_exists should pass bare bucket/path directly to s3fs."""
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        storage = self._make_storage_access(mock_fs)

        result = storage.file_exists("my-bucket/silver/data.parquet")
        assert result is True
        mock_fs.exists.assert_called_once_with("my-bucket/silver/data.parquet")

    def test_file_exists_strips_r2_prefix(self):
        """file_exists should also strip r2:// prefix."""
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        storage = self._make_storage_access(mock_fs)

        result = storage.file_exists("r2://my-bucket/silver/data.parquet")
        assert result is True
        # Should strip r2:// prefix
        call_arg = mock_fs.exists.call_args[0][0]
        assert not call_arg.startswith("r2://")

    def test_get_file_size_with_bare_path(self):
        """get_file_size should work with bare bucket/path."""
        mock_fs = MagicMock()
        mock_fs.size.return_value = 1024
        storage = self._make_storage_access(mock_fs)

        result = storage.get_file_size("my-bucket/gold/output.parquet")
        assert result == 1024
        mock_fs.size.assert_called_once_with("my-bucket/gold/output.parquet")

    def test_get_file_size_strips_r2_prefix(self):
        """get_file_size should strip r2:// prefix."""
        mock_fs = MagicMock()
        mock_fs.size.return_value = 2048
        storage = self._make_storage_access(mock_fs)

        result = storage.get_file_size("r2://my-bucket/gold/output.parquet")
        assert result == 2048
        call_arg = mock_fs.size.call_args[0][0]
        assert not call_arg.startswith("r2://")

    def test_list_files_returns_bare_paths(self):
        """list_files should return bare bucket/path results."""
        mock_fs = MagicMock()
        mock_fs.glob.return_value = [
            "my-bucket/silver/file1.parquet",
            "my-bucket/silver/file2.parquet",
        ]
        storage = self._make_storage_access(mock_fs)

        result = storage.list_files("my-bucket/silver/*.parquet")
        assert len(result) == 2
        # Results should be bare paths (no protocol prefix)
        for r in result:
            assert not r.startswith("r2://")
            assert not r.startswith("s3://")

    def test_list_files_with_r2_prefix(self):
        """list_files with r2:// should work the same way."""
        mock_fs = MagicMock()
        mock_fs.glob.return_value = ["my-bucket/silver/file1.parquet"]
        storage = self._make_storage_access(mock_fs)

        result = storage.list_files("r2://my-bucket/silver/*.parquet")
        assert len(result) >= 1
        # glob should be called with bare path
        call_arg = mock_fs.glob.call_args[0][0]
        assert not call_arg.startswith("r2://")


class TestUploadDownloadJSON:
    """Test JSON upload/download with s3fs mock."""

    def _make_storage_access_with_memory_fs(self):
        """Create StorageAccess with in-memory file storage mock."""
        mock_fs = MagicMock()
        mock_fs._file_contents = {}

        def mock_open_handler(path, mode="r", **kwargs):
            if "w" in mode:
                buffer = io.StringIO()

                class MockWriteContext:
                    def __enter__(self):
                        return buffer

                    def __exit__(self, *args):
                        buffer.seek(0)
                        mock_fs._file_contents[path] = buffer.read()
                        return False

                return MockWriteContext()
            content = mock_fs._file_contents.get(path, "")
            buffer = io.StringIO(content)

            class MockReadContext:
                def __enter__(self):
                    return buffer

                def __exit__(self, *args):
                    return False

            return MockReadContext()

        mock_fs.open.side_effect = mock_open_handler

        with (
            patch("common.storage.filesystem.get_r2_filesystem", return_value=mock_fs),
            patch("common.storage.core.get_r2_filesystem", return_value=mock_fs),
            patch("common.storage.core.ResourceMonitor"),
        ):
            from common.storage.core import StorageAccess

            storage = StorageAccess.__new__(StorageAccess)
            storage.fs = mock_fs
            storage.log = MagicMock()
            storage.duckdb_conn = duckdb.connect(":memory:")
            storage.monitor = MagicMock()
            storage._native_cloud_available = False
            return storage

    def test_upload_json_roundtrip(self):
        """JSON upload then download should preserve data."""
        storage = self._make_storage_access_with_memory_fs()
        test_data = {"farms": [{"cvr": "31373077", "name": "Test Farm"}]}

        storage.upload_json(test_data, "bucket/test.json")
        result = storage.download_json("bucket/test.json")

        assert result == test_data

    def test_danish_characters_roundtrip(self):
        """Danish characters (æøå) should survive upload/download roundtrip."""
        storage = self._make_storage_access_with_memory_fs()
        test_data = {
            "locations": ["København", "Århus", "Ålborg", "Sønderjylland"],
            "special_chars": "æøå ÆØÅ",
            "description": "Landbrugsstyrelsen data",
        }

        storage.upload_json(test_data, "bucket/danish.json")
        result = storage.download_json("bucket/danish.json")

        assert result["locations"] == test_data["locations"]
        assert result["special_chars"] == "æøå ÆØÅ"

    def test_upload_json_with_cvr_numbers(self):
        """CVR numbers with leading zeros should be preserved."""
        storage = self._make_storage_access_with_memory_fs()
        test_data = {
            "companies": [
                {"cvr": "00113115", "name": "Test"},
                {"cvr": "31373077", "name": "Arla"},
            ]
        }

        storage.upload_json(test_data, "bucket/cvr.json")
        result = storage.download_json("bucket/cvr.json")

        assert result["companies"][0]["cvr"] == "00113115"


class TestNativeR2Support:
    """Test detection and use of native R2 DuckDB support."""

    @patch.dict(
        os.environ,
        {
            "R2_ACCESS_KEY_ID": "test-key",
            "R2_SECRET_ACCESS_KEY": "test-secret",
            "R2_ACCOUNT_ID": "abc123def456ghi789jkl012mno345pqr",
        },
    )
    def test_check_native_r2_support_detects_r2_secret(self):
        """_check_native_cloud_support should detect R2 secrets."""
        mock_fs = MagicMock()

        with (
            patch("common.storage.core.get_r2_filesystem", return_value=mock_fs),
            patch("common.storage.core.ResourceMonitor"),
        ):
            from common.storage.core import StorageAccess

            storage = StorageAccess.__new__(StorageAccess)
            storage.fs = mock_fs
            storage.log = MagicMock()
            storage.duckdb_conn = duckdb.connect(":memory:")
            storage.monitor = MagicMock()

            # Setup R2 auth
            from common.storage.filesystem import _setup_native_r2_auth

            auth_result = _setup_native_r2_auth(storage.duckdb_conn)
            assert auth_result is True

            # Now check native support detection
            result = storage._check_native_cloud_support()
            # Should detect the r2 secret
            assert result is True


class TestBackwardCompatibility:
    """Ensure StorageAccess class handles bare and r2:// paths."""

    def test_class_name_is_storage_access(self):
        """Class should still be importable as StorageAccess."""
        from common.storage.core import StorageAccess

        assert StorageAccess is not None
        assert StorageAccess.__name__ == "StorageAccess"

    def test_module_exports_storage_access(self):
        """common.storage module should export StorageAccess."""
        from common.storage import StorageAccess

        assert StorageAccess is not None
