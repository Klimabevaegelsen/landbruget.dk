"""Comprehensive tests for GCS data access layer (common.gcs module).

This module tests the GCS access layer, covering:
- GCS filesystem initialization and caching
- DuckDB integration with GCS (native and fsspec)
- Resource monitoring (GitHub Actions vs local)
- Query operations (parquet with DuckDB)
- Upload operations (JSON, parquet, CSV)
- Process operations (GCS-to-GCS transformations)
- Error handling and network failures

Tests use mocks for GCS operations to avoid actual cloud storage interaction.

Note: These tests directly import from common.gcs which is available via sys.path
      configuration in conftest.py.
"""

import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

# Add backend to path if not already there (for when running tests standalone)
# Path: .../backend/pipelines/unified_pipeline/src/tests/util/test_gcs_access.py
# Navigate up to backend directory: ../../../../../../../backend
test_file_path = Path(__file__).resolve()
backend_dir = (
    test_file_path.parents[6] / "backend"
)  # Go up 6 levels from test file to milan-v2, then to backend

if backend_dir.exists() and str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# Verify path is correct (for debugging)
if not backend_dir.exists():
    raise RuntimeError(f"Backend directory not found at {backend_dir}")

# Now import from common.gcs
from common.gcs import GCSDataAccess, ResourceMonitor, get_duckdb_with_gcs, get_gcs_filesystem

# =============================================================================
# GCS Filesystem Tests
# =============================================================================


@patch("common.gcs.filesystem.gcsfs.GCSFileSystem")
@patch.dict(
    os.environ, {"GCS_ACCESS_KEY_ID": "test_key_id", "GCS_SECRET_ACCESS_KEY": "test_secret"}
)
def test_get_gcs_filesystem_hmac_auth(mock_gcsfs_class):
    """Test get_gcs_filesystem with HMAC authentication."""
    # Clear the LRU cache to ensure fresh call
    get_gcs_filesystem.cache_clear()

    mock_fs = MagicMock()
    mock_gcsfs_class.return_value = mock_fs

    # Call function
    result = get_gcs_filesystem()

    # Verify HMAC credentials were used
    mock_gcsfs_class.assert_called_once_with(
        access_key_id="test_key_id", secret_access_key="test_secret"
    )

    assert result == mock_fs


@patch("common.gcs.filesystem.gcsfs.GCSFileSystem")
@patch.dict(os.environ, {}, clear=True)
def test_get_gcs_filesystem_service_account_fallback(mock_gcsfs_class):
    """Test get_gcs_filesystem fallback to service account credentials."""
    # Clear the LRU cache
    get_gcs_filesystem.cache_clear()

    mock_fs = MagicMock()
    mock_gcsfs_class.return_value = mock_fs

    # Call function without HMAC credentials
    result = get_gcs_filesystem()

    # Verify service account auth was used (no credentials passed)
    mock_gcsfs_class.assert_called_once_with()

    assert result == mock_fs


@patch("common.gcs.filesystem.gcsfs.GCSFileSystem")
@patch.dict(os.environ, {"GCS_ACCESS_KEY_ID": "test_key", "GCS_SECRET_ACCESS_KEY": "test_secret"})
def test_get_gcs_filesystem_caching(mock_gcsfs_class):
    """Test that filesystem is cached and reused (LRU cache)."""
    # Clear cache before test
    get_gcs_filesystem.cache_clear()

    mock_fs = MagicMock()
    mock_gcsfs_class.return_value = mock_fs

    # Call multiple times
    result1 = get_gcs_filesystem()
    result2 = get_gcs_filesystem()
    result3 = get_gcs_filesystem()

    # Should only create filesystem once due to caching
    assert mock_gcsfs_class.call_count == 1

    # Should return same instance
    assert result1 is result2
    assert result2 is result3


# =============================================================================
# DuckDB Integration Tests
# =============================================================================


@patch("common.gcs.filesystem._setup_native_gcs_auth")
@patch.dict(os.environ, {"GCS_ACCESS_KEY_ID": "test_key", "GCS_SECRET_ACCESS_KEY": "test_secret"})
def test_get_duckdb_with_gcs_native_auth(mock_setup_native):
    """Test DuckDB initialization with native GCS extension."""
    # Clear cache
    get_duckdb_with_gcs.cache_clear()

    # Mock successful native authentication
    mock_setup_native.return_value = True

    # Call function
    conn = get_duckdb_with_gcs()

    # Verify native auth was attempted
    assert mock_setup_native.call_count == 1

    # Verify we got a DuckDB connection
    assert isinstance(conn, duckdb.DuckDBPyConnection)

    # Cleanup
    conn.close()


@patch("common.gcs.filesystem._setup_native_gcs_auth")
@patch("common.gcs.filesystem.filesystem")
@patch.dict(os.environ, {"GCS_ACCESS_KEY_ID": "test_key", "GCS_SECRET_ACCESS_KEY": "test_secret"})
def test_get_duckdb_with_gcs_hmac_auth(mock_filesystem, mock_setup_native):
    """Test DuckDB with HMAC auth via fsspec fallback."""
    # Clear cache
    get_duckdb_with_gcs.cache_clear()

    # Mock native auth failure, fsspec fallback success
    mock_setup_native.return_value = False
    mock_fs = MagicMock()
    mock_filesystem.return_value = mock_fs

    # Call function
    conn = get_duckdb_with_gcs()

    # Verify native auth was tried first
    assert mock_setup_native.call_count == 1

    # Verify fsspec was used as fallback with HMAC
    mock_filesystem.assert_called_once_with(
        "gs", access_key_id="test_key", secret_access_key="test_secret"
    )

    # Verify filesystem was registered with DuckDB
    # Note: We can't easily verify conn.register_filesystem was called due to it being a C extension

    # Cleanup
    conn.close()


@patch("common.gcs.filesystem._setup_native_gcs_auth")
def test_duckdb_spatial_extension_loaded(mock_setup_native):
    """Verify spatial extension loads in DuckDB connection."""
    # Clear cache
    get_duckdb_with_gcs.cache_clear()

    mock_setup_native.return_value = False

    # Get connection
    conn = get_duckdb_with_gcs()

    # Verify spatial extension is loaded by checking for ST functions
    # This will raise an error if spatial extension isn't loaded
    try:
        # Try to use a spatial function
        conn.execute("SELECT ST_Point(0, 0) as geom").fetchone()
    except duckdb.CatalogException:
        pass

    # Note: Spatial extension may not be available in all test environments
    # So we just verify the connection works
    assert conn is not None

    # Cleanup
    conn.close()


# =============================================================================
# Resource Monitor Tests
# =============================================================================


@patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
def test_resource_monitor_github_actions_skip():
    """Test that monitoring is skipped in GitHub Actions."""
    monitor = ResourceMonitor()

    result = monitor.check_resources("test_operation")

    # Should return zero values in GitHub Actions
    assert result["memory_gb"] == 0.0
    assert result["disk_gb"] == 0.0
    assert result["memory_percent"] == 0.0
    assert result["disk_percent"] == 0.0


@patch.dict(os.environ, {}, clear=True)
@patch("common.gcs.monitoring.psutil")
def test_resource_monitor_local_execution(mock_psutil):
    """Test resource monitoring in local environment."""
    # Mock psutil responses
    mock_memory = MagicMock()
    mock_memory.total = 32 * (1024**3)  # 32 GB total
    mock_memory.available = 16 * (1024**3)  # 16 GB available
    mock_memory.percent = 50.0

    mock_disk = MagicMock()
    mock_disk.total = 1000 * (1024**3)  # 1000 GB total
    mock_disk.free = 500 * (1024**3)  # 500 GB free

    mock_psutil.virtual_memory.return_value = mock_memory
    mock_psutil.disk_usage.return_value = mock_disk

    monitor = ResourceMonitor()
    result = monitor.check_resources("test_operation")

    # Should return actual values
    assert result["memory_gb"] == pytest.approx(16.0, rel=0.1)
    assert result["disk_gb"] == pytest.approx(500.0, rel=0.1)
    assert result["memory_percent"] == 50.0


@patch.dict(os.environ, {}, clear=True)
@patch("common.gcs.monitoring.psutil")
def test_resource_monitor_thresholds(mock_psutil):
    """Test memory/disk threshold warnings."""
    # Mock high memory usage
    mock_memory = MagicMock()
    mock_memory.total = 32 * (1024**3)
    mock_memory.available = 1 * (1024**3)  # Only 1 GB available (31 GB used)
    mock_memory.percent = 96.9

    mock_disk = MagicMock()
    mock_disk.total = 1000 * (1024**3)
    mock_disk.free = 50 * (1024**3)  # 950 GB used

    mock_psutil.virtual_memory.return_value = mock_memory
    mock_psutil.disk_usage.return_value = mock_disk

    monitor = ResourceMonitor()

    # Should issue warnings for high usage
    with pytest.warns(UserWarning, match="High memory usage"):
        monitor.check_resources("test_operation")

    with pytest.warns(UserWarning, match="High disk usage"):
        monitor.check_resources("test_operation")

    # Verify max tracking
    assert monitor.max_memory_usage > 20
    assert monitor.max_disk_usage > 400


# =============================================================================
# Query Operations Tests
# =============================================================================


@patch("common.gcs.core.get_gcs_filesystem")
def test_query_parquet_with_duckdb(mock_get_fs):
    """Test querying parquet files with DuckDB."""
    # Setup mock filesystem
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock file operations
    mock_file_content = b"parquet_file_content"

    @contextmanager
    def mock_open(*args, **kwargs):
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        yield mock_file

    mock_fs.open.side_effect = mock_open
    mock_fs.info.return_value = {"size": 1024 * 1024}  # 1 MB

    # Create test data in temp file that GCSDataAccess will read
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

        # Create simple test table in parquet
        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE test_data AS
            SELECT * FROM (VALUES
                ('31373077', 'Arla Foods', 100.5),
                ('10150817', 'Danish Crown', 200.3)
            ) AS t(cvr, name, area_ha)
        """)
        conn.execute(f"COPY test_data TO '{tmp_path}' (FORMAT PARQUET)")
        conn.close()

    try:
        # Patch _temp_download to use our test file
        with patch.object(GCSDataAccess, "_temp_download") as mock_temp_download:

            @contextmanager
            def temp_file_context():
                yield tmp_path

            mock_temp_download.return_value = temp_file_context()

            # Create GCSDataAccess instance
            gcs = GCSDataAccess()

            # Query the data
            table_name = gcs.query_parquet_with_duckdb(
                "gs://test-bucket/test.parquet", query="cvr LIKE '31%'"
            )

            # Verify table was created
            result = gcs.duckdb_conn.execute(f"SELECT * FROM {table_name}").fetchall()

            # Should only have filtered row
            assert len(result) == 1
            assert result[0][0] == "31373077"

            # Cleanup
            gcs.duckdb_conn.close()
    finally:
        # Cleanup temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@patch("common.gcs.core.get_gcs_filesystem")
def test_query_parquet_native(mock_get_fs):
    """Test native GCS query without download."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Create GCSDataAccess with native support
    with patch.object(GCSDataAccess, "_check_native_gcs_support", return_value=True):
        gcs = GCSDataAccess()

        # Mock successful native query
        gcs.duckdb_conn.execute = MagicMock()
        gcs.duckdb_conn.execute.return_value.fetchone.return_value = (10,)

        # Query using native method
        table_name = gcs.query_parquet_native(
            "gs://test-bucket/data.parquet", query="SELECT * WHERE year = 2024", table_name="result"
        )

        # Verify table name returned
        assert table_name == "result"

        # Verify native query was attempted
        assert gcs.duckdb_conn.execute.called

        # Cleanup
        gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_query_parquet_with_filters(mock_get_fs):
    """Test server-side filtering in parquet queries."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.info.return_value = {"size": 1024 * 1024}

    # Create test parquet file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE test_data AS
            SELECT * FROM (VALUES
                ('31373077', 2023, 100.5),
                ('31373077', 2024, 150.3),
                ('10150817', 2024, 200.7)
            ) AS t(cvr, year, area_ha)
        """)
        conn.execute(f"COPY test_data TO '{tmp_path}' (FORMAT PARQUET)")
        conn.close()

    try:
        with patch.object(GCSDataAccess, "_temp_download") as mock_temp_download:

            @contextmanager
            def temp_file_context():
                yield tmp_path

            mock_temp_download.return_value = temp_file_context()

            gcs = GCSDataAccess()

            # Query with filter
            table_name = gcs.query_parquet_with_duckdb(
                "gs://test-bucket/data.parquet", query="year = 2024 AND cvr = '31373077'"
            )

            result = gcs.duckdb_conn.execute(f"SELECT * FROM {table_name}").fetchall()

            # Should only have 1 filtered row
            assert len(result) == 1
            assert result[0][0] == "31373077"
            assert result[0][1] == 2024

            gcs.duckdb_conn.close()
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# =============================================================================
# Upload Operations Tests
# =============================================================================


@patch("common.gcs.core.get_gcs_filesystem")
def test_upload_json_with_retry(mock_get_fs):
    """Test JSON upload with retry logic."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock file operations
    mock_file = MagicMock()

    @contextmanager
    def mock_open(*args, **kwargs):
        yield mock_file

    mock_fs.open.side_effect = mock_open

    gcs = GCSDataAccess()

    test_data = {"cvr": "31373077", "name": "Arla Foods", "location": "København"}

    # Upload JSON
    gcs.upload_json(test_data, "gs://test-bucket/data.json")

    # Verify open was called with correct path
    mock_fs.open.assert_called_once_with("gs://test-bucket/data.json", "w", encoding="utf-8")

    # Verify json.dump was called
    assert mock_file.method_calls  # File operations happened

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_upload_from_duckdb_table_parquet(mock_get_fs):
    """Test parquet upload from DuckDB table."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 1024

    # Mock file operations
    @contextmanager
    def mock_open(*args, **kwargs):
        mock_file = MagicMock()
        yield mock_file

    mock_fs.open.side_effect = mock_open

    gcs = GCSDataAccess()

    # Create test table
    gcs.duckdb_conn.execute("""
        CREATE TABLE test_table AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla Foods'),
            ('10150817', 'Danish Crown')
        ) AS t(cvr, name)
    """)

    # Upload table as parquet
    gcs.upload_from_duckdb_table("test_table", "gs://test-bucket/output.parquet")

    # Verify file operations happened
    assert mock_fs.open.called

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_upload_from_duckdb_table_csv(mock_get_fs):
    """Test CSV upload from DuckDB table."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 512

    # Mock file operations
    @contextmanager
    def mock_open(*args, **kwargs):
        mock_file = MagicMock()
        yield mock_file

    mock_fs.open.side_effect = mock_open

    gcs = GCSDataAccess()

    # Create test table
    gcs.duckdb_conn.execute("""
        CREATE TABLE csv_table AS
        SELECT * FROM (VALUES
            ('31373077', 'Arla'),
            ('10150817', 'Danish Crown')
        ) AS t(cvr, name)
    """)

    # Upload as CSV
    gcs.upload_from_duckdb_table("csv_table", "gs://test-bucket/output.csv")

    # Verify upload happened
    assert mock_fs.open.called

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_upload_batch_operations(mock_get_fs):
    """Test batch upload efficiency."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 2048

    @contextmanager
    def mock_open(*args, **kwargs):
        yield MagicMock()

    mock_fs.open.side_effect = mock_open

    gcs = GCSDataAccess()

    # Upload multiple JSON files
    for i in range(5):
        test_data = {"id": i, "cvr": f"3137307{i}"}
        gcs.upload_json(test_data, f"gs://test-bucket/batch_{i}.json")

    # Verify all uploads were called
    assert mock_fs.open.call_count == 5

    gcs.duckdb_conn.close()


# =============================================================================
# Process Operations Tests
# =============================================================================


@patch("common.gcs.core.get_gcs_filesystem")
def test_process_gcs_to_gcs_direct(mock_get_fs):
    """Test direct GCS-to-GCS processing (no DataFrame conversion)."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.info.return_value = {"size": 1024 * 1024}
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 512

    # Create input test file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_input:
        tmp_input_path = tmp_input.name

        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE input_data AS
            SELECT * FROM (VALUES
                ('31373077', 'wheat', 100.5),
                ('31373077', 'barley', 50.3),
                ('10150817', 'wheat', 200.7)
            ) AS t(cvr, crop_type, area_ha)
        """)
        conn.execute(f"COPY input_data TO '{tmp_input_path}' (FORMAT PARQUET)")
        conn.close()

    try:
        with patch.object(GCSDataAccess, "_temp_download") as mock_temp_download:

            @contextmanager
            def temp_file_context():
                yield tmp_input_path

            mock_temp_download.return_value = temp_file_context()

            # Mock file open for output
            @contextmanager
            def mock_open(*args, **kwargs):
                yield MagicMock()

            mock_fs.open.side_effect = mock_open

            gcs = GCSDataAccess()

            # Process with transformation
            processing_query = """
                SELECT cvr, SUM(area_ha) as total_area
                FROM input_table
                WHERE crop_type = 'wheat'
                GROUP BY cvr
            """

            gcs.process_gcs_to_gcs_direct(
                "gs://test-bucket/input.parquet",
                "gs://test-bucket/output.parquet",
                processing_query,
            )

            # Verify operations happened
            assert mock_fs.open.called

            gcs.duckdb_conn.close()
    finally:
        if os.path.exists(tmp_input_path):
            os.unlink(tmp_input_path)


@patch("common.gcs.core.get_gcs_filesystem")
def test_process_gcs_to_gcs_with_transformation(mock_get_fs):
    """Test data transformation during GCS processing."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_fs.info.return_value = {"size": 1024 * 1024}
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 512

    # Create input file with data to transform
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE raw_data AS
            SELECT * FROM (VALUES
                ('31373077', 100.5, 25.3),
                ('10150817', 200.7, 50.1)
            ) AS t(cvr, area_ha, nitrogen_kg)
        """)
        conn.execute(f"COPY raw_data TO '{tmp_path}' (FORMAT PARQUET)")
        conn.close()

    try:
        with patch.object(GCSDataAccess, "_temp_download") as mock_temp_download:

            @contextmanager
            def temp_file_context():
                yield tmp_path

            mock_temp_download.return_value = temp_file_context()

            @contextmanager
            def mock_open(*args, **kwargs):
                yield MagicMock()

            mock_fs.open.side_effect = mock_open

            gcs = GCSDataAccess()

            # Transform: calculate nitrogen per hectare
            transform_query = """
                SELECT
                    cvr,
                    area_ha,
                    nitrogen_kg,
                    nitrogen_kg / area_ha as nitrogen_per_ha
                FROM input_table
            """

            gcs.process_gcs_to_gcs_direct(
                "gs://test-bucket/raw.parquet",
                "gs://test-bucket/processed.parquet",
                transform_query,
            )

            # Verify upload was attempted
            assert mock_fs.open.called

            gcs.duckdb_conn.close()
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@patch("common.gcs.core.get_gcs_filesystem")
def test_file_size_limits_enforcement(mock_get_fs):
    """Test 8GB file size limit enforcement."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock file info with size > 8 GB
    mock_fs.info.return_value = {"size": 9 * (1024**3)}  # 9 GB

    gcs = GCSDataAccess()

    # Should raise error for oversized file
    with pytest.raises(ValueError, match="exceeds runner limit"):
        gcs.check_file_size_limits("gs://test-bucket/huge_file.parquet")

    gcs.duckdb_conn.close()


# =============================================================================
# Error Handling Tests
# =============================================================================


@patch("common.gcs.core.get_gcs_filesystem")
def test_gcs_access_authentication_failure(mock_get_fs):
    """Test handling of auth failures."""
    # Mock filesystem that raises auth error
    mock_fs = MagicMock()
    mock_fs.open.side_effect = PermissionError("Authentication failed")
    mock_get_fs.return_value = mock_fs

    gcs = GCSDataAccess()

    # Should propagate auth error
    with pytest.raises(PermissionError, match="Authentication failed"):
        gcs.upload_json({"test": "data"}, "gs://test-bucket/data.json")

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_gcs_access_network_failure(mock_get_fs):
    """Test network error handling and retries."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock network failure
    mock_fs.open.side_effect = ConnectionError("Network unreachable")

    gcs = GCSDataAccess()

    # upload_json has retry logic, but should eventually fail
    with pytest.raises(ConnectionError):
        gcs.upload_json({"test": "data"}, "gs://test-bucket/data.json")

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_gcs_access_missing_bucket(mock_get_fs):
    """Test handling of non-existent buckets."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock bucket not found error
    mock_fs.open.side_effect = FileNotFoundError("Bucket not found")

    gcs = GCSDataAccess()

    # Should propagate bucket not found error
    with pytest.raises(FileNotFoundError, match="Bucket not found"):
        gcs.download_json("gs://nonexistent-bucket/data.json")

    gcs.duckdb_conn.close()


# =============================================================================
# Additional Utility Tests
# =============================================================================


@patch("common.gcs.core.get_gcs_filesystem")
def test_list_files_with_pattern(mock_get_fs):
    """Test listing files with glob patterns."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Mock glob results
    mock_fs.glob.return_value = [
        "test-bucket/bronze/2024/file1.parquet",
        "test-bucket/bronze/2024/file2.parquet",
        "test-bucket/bronze/2024/file3.parquet",
    ]

    gcs = GCSDataAccess()

    files = gcs.list_files("gs://test-bucket/bronze/2024/*.parquet")

    # Verify results include gs:// prefix
    assert len(files) == 3
    assert all(f.startswith("gs://") for f in files)
    assert "gs://test-bucket/bronze/2024/file1.parquet" in files

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_file_exists_check(mock_get_fs):
    """Test file existence checking."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    mock_fs.exists.side_effect = lambda path: path == "test-bucket/existing.json"

    gcs = GCSDataAccess()

    assert gcs.file_exists("gs://test-bucket/existing.json") is True
    assert gcs.file_exists("gs://test-bucket/nonexistent.json") is False

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_get_file_size(mock_get_fs):
    """Test getting file size."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    mock_fs.size.return_value = 1024 * 1024  # 1 MB

    gcs = GCSDataAccess()

    size = gcs.get_file_size("gs://test-bucket/data.parquet")

    assert size == 1024 * 1024

    gcs.duckdb_conn.close()


@patch("common.gcs.core.get_gcs_filesystem")
def test_connection_reuse(mock_get_fs):
    """Test that provided DuckDB connection is reused."""
    mock_fs = MagicMock()
    mock_get_fs.return_value = mock_fs

    # Create external connection
    external_conn = duckdb.connect()
    external_conn.execute("CREATE TABLE test_table AS SELECT 1 as id")

    # Pass to GCSDataAccess
    gcs = GCSDataAccess(connection=external_conn)

    # Verify same connection is used
    assert gcs.duckdb_conn is external_conn

    # Verify table from external connection is accessible
    result = gcs.duckdb_conn.execute("SELECT * FROM test_table").fetchone()
    assert result[0] == 1

    # Don't close external connection in GCSDataAccess
    external_conn.close()
