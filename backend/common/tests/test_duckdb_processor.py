"""Tests for SharedDuckDBProcessor and PipelineProcessor.

Tests the shared DuckDB processor base classes used across all data pipelines.
Covers connection lifecycle, table operations, file I/O, and error handling.
"""

import csv
from unittest.mock import MagicMock, patch

import duckdb
import pytest


class TestSharedDuckDBProcessorLifecycle:
    """Test connection lifecycle and context manager behavior."""

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_context_manager_creates_and_closes_connection(self, mock_cloud_auth):
        """Verify with SharedDuckDBProcessor() as proc: creates a connection and closes it on exit."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            # Connection should be usable inside context
            result = proc.conn.execute("SELECT 1").fetchone()
            assert result[0] == 1

        # After exiting context, connection should be closed
        with pytest.raises(duckdb.ConnectionException):
            proc.conn.execute("SELECT 1")

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_init_sets_dataset_name(self, mock_cloud_auth):
        """Verify dataset_name is stored correctly."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:", dataset_name="test_data") as proc:
            assert proc.dataset_name == "test_data"

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_close_is_idempotent(self, mock_cloud_auth):
        """Calling close() multiple times should not raise."""
        from common.duckdb_processor import SharedDuckDBProcessor

        proc = SharedDuckDBProcessor(db_path=":memory:")
        proc.close()
        # Second close should not raise
        proc.close()


class TestTableExists:
    """Test table_exists method."""

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_table_exists_returns_false_for_nonexistent(self, mock_cloud_auth):
        """table_exists should return False for a table that does not exist."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            assert proc.table_exists("fake_table") is False

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_table_exists_returns_true_after_creation(self, mock_cloud_auth):
        """table_exists should return True after creating a table."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE my_table (id INTEGER, name VARCHAR)")
            assert proc.table_exists("my_table") is True


class TestFileImportExport:
    """Test creating tables from files and saving tables to files."""

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_create_table_from_csv(self, mock_cloud_auth, tmp_path):
        """Create a table from a CSV file and verify the row count matches."""
        from common.duckdb_processor import SharedDuckDBProcessor

        # Create a temp CSV
        csv_path = tmp_path / "test_data.csv"
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "value"])
            writer.writerow([1, "alpha", 10.5])
            writer.writerow([2, "beta", 20.3])
            writer.writerow([3, "gamma", 30.1])

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            table_name = proc.create_table_from_csv(str(csv_path), table_name="csv_test")
            assert table_name == "csv_test"
            assert proc.table_exists("csv_test") is True
            assert proc.get_row_count("csv_test") == 3

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_create_table_from_parquet(self, mock_cloud_auth, tmp_path):
        """Create a table from a Parquet file and verify the row count matches."""
        from common.duckdb_processor import SharedDuckDBProcessor

        # Create a temp parquet via DuckDB
        parquet_path = tmp_path / "test_data.parquet"
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE src (id INTEGER, label VARCHAR)")
        conn.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        conn.execute(f"COPY src TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            table_name = proc.create_table_from_parquet(str(parquet_path), table_name="parquet_test")
            assert table_name == "parquet_test"
            assert proc.get_row_count("parquet_test") == 4

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_save_table_to_parquet(self, mock_cloud_auth, tmp_path):
        """Save a table to Parquet format and verify the file contains correct data."""
        from common.duckdb_processor import SharedDuckDBProcessor

        output_path = tmp_path / "output.parquet"

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE export_test (id INTEGER, value DOUBLE)")
            proc.conn.execute("INSERT INTO export_test VALUES (1, 1.1), (2, 2.2)")
            proc.save_table_to_parquet("export_test", str(output_path))

        assert output_path.exists()

        # Verify data by reading back
        verify_conn = duckdb.connect(":memory:")
        rows = verify_conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
        assert rows[0] == 2
        verify_conn.close()

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_save_table_to_csv(self, mock_cloud_auth, tmp_path):
        """Save a table to CSV format and verify the file contains correct data."""
        from common.duckdb_processor import SharedDuckDBProcessor

        output_path = tmp_path / "output.csv"

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE csv_export (id INTEGER, name VARCHAR)")
            proc.conn.execute("INSERT INTO csv_export VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')")
            proc.save_table_to_csv("csv_export", str(output_path))

        assert output_path.exists()

        # Verify CSV content
        with output_path.open() as f:
            reader = csv.reader(f)
            rows = list(reader)
        # Header + 3 data rows
        assert len(rows) == 4
        assert rows[0] == ["id", "name"]

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_create_table_from_csv_auto_generates_name(self, mock_cloud_auth, tmp_path):
        """When no table_name is provided, an auto-generated name should be used."""
        from common.duckdb_processor import SharedDuckDBProcessor

        csv_path = tmp_path / "auto_name.csv"
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["col1"])
            writer.writerow(["val1"])

        with SharedDuckDBProcessor(db_path=":memory:", dataset_name="myds") as proc:
            table_name = proc.create_table_from_csv(str(csv_path))
            assert table_name.startswith("myds_")
            assert proc.table_exists(table_name) is True


class TestTableOperations:
    """Test table inspection and manipulation methods."""

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_get_table_info_returns_column_metadata(self, mock_cloud_auth):
        """get_table_info should return column names and types."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE info_test (id INTEGER, name VARCHAR, value DOUBLE)")

            info = proc.get_table_info("info_test")

            assert isinstance(info, list)
            assert len(info) == 3

            column_names = [col["column_name"] for col in info]
            assert "id" in column_names
            assert "name" in column_names
            assert "value" in column_names

            # Check that types are present
            for col in info:
                assert "column_type" in col
                assert col["column_type"] is not None

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_get_row_count(self, mock_cloud_auth):
        """get_row_count should return the accurate number of rows."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE count_test (id INTEGER)")
            assert proc.get_row_count("count_test") == 0

            proc.conn.execute("INSERT INTO count_test VALUES (1), (2), (3), (4), (5)")
            assert proc.get_row_count("count_test") == 5

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_execute_query_returns_results(self, mock_cloud_auth):
        """execute_query should return query results as a list of tuples."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE query_test (id INTEGER, label VARCHAR)")
            proc.conn.execute("INSERT INTO query_test VALUES (1, 'x'), (2, 'y')")

            results = proc.execute_query("SELECT id, label FROM query_test ORDER BY id")

            assert len(results) == 2
            assert results[0] == (1, "x")
            assert results[1] == (2, "y")

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_drop_table_if_exists(self, mock_cloud_auth):
        """drop_table_if_exists should remove a table and not error on missing tables."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE drop_test (id INTEGER)")
            assert proc.table_exists("drop_test") is True

            proc.drop_table_if_exists("drop_test")
            assert proc.table_exists("drop_test") is False

            # Should not raise when dropping a non-existent table
            proc.drop_table_if_exists("nonexistent_table")

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_drop_table_if_exists_nonexistent(self, mock_cloud_auth):
        """drop_table_if_exists on a nonexistent table should not raise."""
        from common.duckdb_processor import SharedDuckDBProcessor

        with SharedDuckDBProcessor(db_path=":memory:") as proc:
            # Should complete without exception
            proc.drop_table_if_exists("table_that_never_existed")


class TestPipelineProcessor:
    """Test PipelineProcessor-specific functionality."""

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_safe_execute_returns_results(self, mock_cloud_auth):
        """safe_execute should return query results on success."""
        from common.duckdb_processor import PipelineProcessor

        with PipelineProcessor(db_path=":memory:") as proc:
            proc.conn.execute("CREATE TABLE safe_test (id INTEGER, val VARCHAR)")
            proc.conn.execute("INSERT INTO safe_test VALUES (1, 'a'), (2, 'b')")

            results = proc.safe_execute("SELECT * FROM safe_test ORDER BY id", description="test query")

            assert results is not None
            assert len(results) == 2
            assert results[0] == (1, "a")

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_safe_execute_returns_none_on_error(self, mock_cloud_auth):
        """safe_execute should return None when the query fails, not raise."""
        from common.duckdb_processor import PipelineProcessor

        with PipelineProcessor(db_path=":memory:") as proc:
            result = proc.safe_execute(
                "SELECT * FROM nonexistent_table_xyz",
                description="bad query",
            )
            assert result is None

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_safe_execute_logs_errors(self, mock_cloud_auth):
        """safe_execute should log errors through the logger when a query fails."""
        from common.duckdb_processor import PipelineProcessor

        mock_logger = MagicMock()

        with PipelineProcessor(db_path=":memory:", logger=mock_logger) as proc:
            proc.safe_execute("INVALID SQL HERE!!!", description="broken query")

        # Should have logged the error
        mock_logger.error.assert_called_once()
        error_msg = mock_logger.error.call_args[0][0]
        assert "broken query" in error_msg

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_safe_execute_logs_success(self, mock_cloud_auth):
        """safe_execute should log info messages on success."""
        from common.duckdb_processor import PipelineProcessor

        mock_logger = MagicMock()

        with PipelineProcessor(db_path=":memory:", logger=mock_logger) as proc:
            proc.conn.execute("CREATE TABLE log_test (id INTEGER)")
            proc.safe_execute("SELECT * FROM log_test", description="read test")

        # Should have logged info (start + completion)
        assert mock_logger.info.call_count == 2

    @patch("common.duckdb_processor.setup_duckdb_cloud_auth")
    def test_pipeline_processor_without_logger(self, mock_cloud_auth):
        """PipelineProcessor should work fine without a logger."""
        from common.duckdb_processor import PipelineProcessor

        with PipelineProcessor(db_path=":memory:") as proc:
            assert proc.logger is None
            # log methods should not raise
            proc.log_info("test info")
            proc.log_warning("test warning")
            proc.log_error("test error")
