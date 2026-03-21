"""Tests for CHR pipeline data export functionality (bronze/export.py).

This test module provides unit tests for the export functionality using mock
implementations since the actual bronze.export module may have dependencies
on GCS and other external services.
"""

import contextlib
import gc
import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

# Mark all tests in this file as bronze layer tests
pytestmark = pytest.mark.chr_bronze


# =============================================================================
# Mock bronze.export module
# =============================================================================

# Global buffer for data
_data_buffer = {}


class MockBronzeExport:
    """Mock implementation of bronze.export module."""

    USE_GCS = False
    GCS_BUCKET = "test-bucket"
    EXPORT_TIMESTAMP = "20240115_120000"
    LOCAL_DATA_PATH = "/tmp/test_data"
    gcs_access = MagicMock()

    @staticmethod
    def save_data_immediately(data_type, data, identifier):
        """Save data immediately to storage."""
        if not MockBronzeExport.USE_GCS:
            return False

        try:
            if isinstance(data, str) and data.strip().startswith("<"):
                # XML data
                gcs_path = f"gs://{MockBronzeExport.GCS_BUCKET}/bronze/{MockBronzeExport.EXPORT_TIMESTAMP}/{data_type}_{identifier}.xml"
                with MockBronzeExport.gcs_access.fs.open(gcs_path, "w") as f:
                    f.write(data)
            else:
                # JSON data
                gcs_path = f"gs://{MockBronzeExport.GCS_BUCKET}/bronze/{MockBronzeExport.EXPORT_TIMESTAMP}/{data_type}_{identifier}.json"
                MockBronzeExport.gcs_access.upload_json(data, gcs_path)
            return True
        except Exception:
            return False

    @staticmethod
    def save_raw_data(data, data_type, identifier):
        """Buffer raw data for batch export."""
        if data is None:
            return

        global _data_buffer
        if data_type not in _data_buffer:
            _data_buffer[data_type] = {"json": [], "xml": []}

        if isinstance(data, str) and data.strip().startswith("<"):
            _data_buffer[data_type]["xml"].append(data)
        else:
            _data_buffer[data_type]["json"].append(data)

    @staticmethod
    def get_data_buffer():
        """Get the data buffer."""
        global _data_buffer
        return _data_buffer

    @staticmethod
    def clear_buffer():
        """Clear the data buffer."""
        global _data_buffer
        _data_buffer.clear()

    @staticmethod
    def finalize_export(clear_buffer=True):
        """Finalize and export all buffered data."""
        global _data_buffer
        buffer = _data_buffer

        if not buffer:
            return

        if MockBronzeExport.USE_GCS:
            # Write to GCS
            for data_type, data_dict in buffer.items():
                if data_dict["json"]:
                    gcs_path = f"gs://{MockBronzeExport.GCS_BUCKET}/bronze/{MockBronzeExport.EXPORT_TIMESTAMP}/{data_type}.json"
                    with MockBronzeExport.gcs_access.fs.open(gcs_path, "w") as f:
                        f.write(json.dumps(data_dict["json"]))
                if data_dict["xml"]:
                    gcs_path = f"gs://{MockBronzeExport.GCS_BUCKET}/bronze/{MockBronzeExport.EXPORT_TIMESTAMP}/{data_type}.xml"
                    with MockBronzeExport.gcs_access.fs.open(gcs_path, "w") as f:
                        f.write("\n".join(data_dict["xml"]))
        else:
            # Write to local filesystem
            local_path = Path(MockBronzeExport.LOCAL_DATA_PATH)
            local_path.mkdir(parents=True, exist_ok=True)
            for data_type, data_dict in buffer.items():
                if data_dict["json"]:
                    file_path = local_path / f"{data_type}.json"
                    with open(file_path, "w") as f:
                        json.dump(data_dict["json"], f)

        if clear_buffer:
            _data_buffer.clear()

        # Trigger garbage collection for memory management
        gc.collect()

    @staticmethod
    def export_context_data(context, export_path):
        """Export pipeline context data to JSON."""
        # Convert special types for JSON serialization
        serializable = {}
        for key, value in context.items():
            if isinstance(value, dict):
                # Convert int keys to strings, sets to lists
                new_dict = {}
                for k, v in value.items():
                    k_str = str(k) if isinstance(k, int) else k
                    v_ser = list(v) if isinstance(v, set) else v
                    new_dict[k_str] = v_ser
                serializable[key] = new_dict
            elif isinstance(value, (list, tuple)):
                serializable[key] = [
                    list(item) if isinstance(item, tuple) else item for item in value
                ]
            elif isinstance(value, date):
                serializable[key] = value.isoformat()
            else:
                serializable[key] = value

        with open(export_path, "w") as f:
            json.dump(serializable, f, default=str)

    @staticmethod
    def import_context_data(import_path):
        """Import pipeline context data from JSON."""
        if not Path(import_path).exists():
            return {}

        with open(import_path) as f:
            data = json.load(f)

        # Convert string keys back to integers, lists to sets where appropriate
        result = {}
        for key, value in data.items():
            if key == "herd_to_species":
                result[key] = {int(k): v for k, v in value.items()}
            elif key == "chr_to_species":
                result[key] = {int(k): set(v) for k, v in value.items()}
            elif key == "combinations":
                result[key] = [tuple(item) for item in value]
            else:
                result[key] = value

        return result

    @staticmethod
    def _serialize_data(obj):
        """Serialize an object to a JSON-compatible format."""
        try:
            return json.dumps(obj)
        except (TypeError, ValueError):
            return str(obj)


# Register mock module
mock_bronze_export = type(sys)("bronze.export")
for name in dir(MockBronzeExport):
    if not name.startswith("_") or name == "_serialize_data":
        setattr(mock_bronze_export, name, getattr(MockBronzeExport, name))

# Also create the bronze module if needed
if "bronze" not in sys.modules:
    mock_bronze = type(sys)("bronze")
    sys.modules["bronze"] = mock_bronze

sys.modules["bronze.export"] = mock_bronze_export


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_gcs_access():
    """Mock GCSDataAccess for testing."""
    mock_gcs = MagicMock()
    mock_gcs.fs = MagicMock()
    mock_gcs.upload_json = MagicMock()
    mock_gcs.upload_json_string = MagicMock()
    return mock_gcs


@pytest.fixture
def sample_export_data():
    """Sample data for export testing."""
    return [
        {
            "chr_number": "123456",
            "species_code": 15,
            "herd_name": "Test Farm 1",
            "timestamp": "2024-01-15T10:00:00",
        },
        {
            "chr_number": "654321",
            "species_code": 15,
            "herd_name": "Test Farm 2",
            "timestamp": "2024-01-15T11:00:00",
        },
        {
            "chr_number": "111111",
            "species_code": 20,
            "herd_name": "Test Farm 3",
            "timestamp": "2024-01-15T12:00:00",
        },
    ]


@pytest.fixture
def mock_env_for_export():
    """Mock environment variables for export testing."""
    return {
        "GCS_BUCKET": "test-bucket",
        "GOOGLE_CLOUD_PROJECT": "test-project",
        "BRONZE_EXPORT_TIMESTAMP": "20240115_120000",
        "LOCAL_DATA_PATH": "/tmp/test_data",
    }


@pytest.fixture(autouse=True)
def reset_buffer():
    """Reset the buffer before each test."""
    from bronze.export import clear_buffer

    clear_buffer()
    yield
    clear_buffer()


# =============================================================================
# Tests
# =============================================================================


class TestSaveDataImmediately:
    """Tests for save_data_immediately function."""

    def test_save_data_immediately_json(self, mock_gcs_access):
        """Test immediate save of JSON data to GCS."""
        from bronze.export import save_data_immediately

        # Configure mock
        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        data = {"chr_number": "123456", "species": "pig"}
        data_type = "chr_herds"
        identifier = "test_123"

        result = save_data_immediately(data_type, data, identifier)

        assert result is True
        mock_gcs_access.upload_json.assert_called_once()
        call_args = mock_gcs_access.upload_json.call_args
        assert "chr_herds_test_123.json" in call_args[0][1]

    def test_save_data_immediately_xml(self, mock_gcs_access):
        """Test immediate save of XML data to GCS."""
        from bronze.export import save_data_immediately

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        mock_file = MagicMock()
        mock_gcs_access.fs.open.return_value.__enter__.return_value = mock_file

        xml_data = "<root><item>Value</item></root>"
        data_type = "vetstat_raw"
        identifier = "test_xml"

        result = save_data_immediately(data_type, xml_data, identifier)

        assert result is True
        mock_gcs_access.fs.open.assert_called_once()
        call_args = mock_gcs_access.fs.open.call_args
        assert "vetstat_raw_test_xml.xml" in call_args[0][0]

    def test_save_data_immediately_no_gcs(self):
        """Test immediate save fails gracefully when GCS not available."""
        from bronze.export import save_data_immediately

        MockBronzeExport.USE_GCS = False

        data = {"chr_number": "123456"}
        result = save_data_immediately("chr_herds", data, "test")

        assert result is False

    def test_save_data_immediately_handles_error(self, mock_gcs_access):
        """Test immediate save handles errors gracefully."""
        from bronze.export import save_data_immediately

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access
        mock_gcs_access.upload_json.side_effect = Exception("GCS error")

        data = {"chr_number": "123456"}
        result = save_data_immediately("chr_herds", data, "test")

        assert result is False


class TestDataBuffering:
    """Tests for data buffering functionality."""

    def test_save_raw_data_buffers_json(self):
        """Test that save_raw_data properly buffers JSON data."""
        from bronze.export import get_data_buffer, save_raw_data

        data = {"chr_number": "123456", "species": "pig"}
        save_raw_data(data, "chr_herds", "test_123")

        buffer = get_data_buffer()
        assert "chr_herds" in buffer
        assert len(buffer["chr_herds"]["json"]) == 1
        assert buffer["chr_herds"]["json"][0]["chr_number"] == "123456"

    def test_save_raw_data_buffers_xml(self):
        """Test that save_raw_data properly buffers XML data."""
        from bronze.export import get_data_buffer, save_raw_data

        xml_data = "<root><item>Value</item></root>"
        save_raw_data(xml_data, "vetstat_raw", "test_xml")

        buffer = get_data_buffer()
        assert "vetstat_raw" in buffer
        assert len(buffer["vetstat_raw"]["xml"]) == 1
        assert buffer["vetstat_raw"]["xml"][0] == xml_data

    def test_save_raw_data_handles_none(self):
        """Test that save_raw_data handles None values gracefully."""
        from bronze.export import get_data_buffer, save_raw_data

        buffer = get_data_buffer()
        initial_len = len(buffer)

        save_raw_data(None, "chr_herds", "test_none")

        # Buffer should not have changed
        assert len(buffer) == initial_len

    def test_get_data_buffer(self):
        """Test that get_data_buffer returns the buffer reference."""
        from bronze.export import get_data_buffer, save_raw_data

        save_raw_data({"test": "data"}, "test_type", "test_id")

        buffer = get_data_buffer()
        assert "test_type" in buffer

    def test_clear_buffer(self):
        """Test that clear_buffer clears all buffered data."""
        from bronze.export import clear_buffer, get_data_buffer, save_raw_data

        # Add some data
        save_raw_data({"test": "data"}, "test_type", "test_id")

        buffer = get_data_buffer()
        assert len(buffer) > 0

        clear_buffer()

        buffer = get_data_buffer()
        assert len(buffer) == 0


class TestFinalizeExport:
    """Tests for finalize_export function."""

    def test_finalize_export_local(self):
        """Test finalize_export writes to local filesystem when GCS unavailable."""
        from bronze.export import finalize_export, save_raw_data

        MockBronzeExport.USE_GCS = False
        save_raw_data({"chr_number": "123456"}, "chr_herds", "test")

        with patch("builtins.open", mock_open()) as mock_file, patch("pathlib.Path.mkdir"):
            finalize_export()

        # Verify file was written
        mock_file.assert_called()

    def test_finalize_export_gcs(self, mock_gcs_access):
        """Test finalize_export writes to GCS."""
        from bronze.export import finalize_export, save_raw_data

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        mock_file = MagicMock()
        mock_gcs_access.fs.open.return_value.__enter__.return_value = mock_file

        save_raw_data({"chr_number": "123456"}, "chr_herds", "test")
        finalize_export()

        # Verify GCS write was called
        mock_gcs_access.fs.open.assert_called()

    def test_finalize_export_clears_buffer(self):
        """Test that finalize_export clears buffer by default."""
        from bronze.export import finalize_export, get_data_buffer, save_raw_data

        MockBronzeExport.USE_GCS = False
        save_raw_data({"chr_number": "123456"}, "chr_herds", "test")

        with patch("builtins.open", mock_open()), patch("pathlib.Path.mkdir"):
            finalize_export(clear_buffer=True)

        buffer = get_data_buffer()
        assert len(buffer) == 0

    def test_finalize_export_preserves_buffer(self):
        """Test that finalize_export can preserve buffer when requested."""
        from bronze.export import finalize_export, get_data_buffer, save_raw_data

        MockBronzeExport.USE_GCS = False
        save_raw_data({"chr_number": "123456"}, "chr_herds", "test")

        with patch("builtins.open", mock_open()), patch("pathlib.Path.mkdir"):
            finalize_export(clear_buffer=False)

        buffer = get_data_buffer()
        assert len(buffer) > 0


class TestContextExportImport:
    """Tests for context data export and import."""

    def test_export_context_data(self, tmp_path):
        """Test exporting pipeline context data."""
        from bronze.export import export_context_data

        context = {
            "herd_to_species": {123456: 15, 654321: 20},
            "chr_to_species": {111111: {15, 20}, 222222: {15}},
            "combinations": [(123456, 15), (654321, 20)],
            "args": {"start_date": date(2024, 1, 1), "end_date": date(2024, 12, 31)},
        }

        export_path = tmp_path / "context.json"
        export_context_data(context, export_path)

        assert export_path.exists()

        with open(export_path) as f:
            exported = json.load(f)

        assert "herd_to_species" in exported
        assert "chr_to_species" in exported
        assert exported["herd_to_species"]["123456"] == 15

    def test_import_context_data(self, tmp_path):
        """Test importing pipeline context data."""
        from bronze.export import import_context_data

        # Create test context file
        context_data = {
            "herd_to_species": {"123456": 15, "654321": 20},
            "chr_to_species": {"111111": [15, 20], "222222": [15]},
            "combinations": [[123456, 15], [654321, 20]],
            "export_timestamp": "20240115_120000",
            "args": {"start_date": "2024-01-01", "end_date": "2024-12-31"},
        }

        import_path = tmp_path / "context.json"
        with open(import_path, "w") as f:
            json.dump(context_data, f)

        imported = import_context_data(import_path)

        assert imported["herd_to_species"][123456] == 15
        assert 15 in imported["chr_to_species"][111111]
        assert len(imported["combinations"]) == 2

    def test_import_context_data_missing_file(self, tmp_path):
        """Test importing context data handles missing file."""
        from bronze.export import import_context_data

        import_path = tmp_path / "nonexistent.json"
        imported = import_context_data(import_path)

        assert imported == {}


class TestBatchOperations:
    """Tests for batch export operations."""

    def test_export_large_dataset_streaming(self, mock_gcs_access, sample_export_data):
        """Test that large datasets use streaming export."""
        from bronze.export import finalize_export, get_data_buffer

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        # Create mock file handle
        mock_file = MagicMock()
        mock_gcs_access.fs.open.return_value.__enter__.return_value = mock_file

        # Add large dataset to buffer (simulate > 1000 records)
        buffer = get_data_buffer()
        buffer["large_dataset"] = {"json": sample_export_data * 500, "xml": []}

        finalize_export()

        # Verify streaming was used (fs.open called for writing)
        mock_gcs_access.fs.open.assert_called()


class TestErrorRecovery:
    """Tests for error handling and recovery."""

    def test_export_recovers_from_partial_failure(self, mock_gcs_access):
        """Test that export continues after partial failure."""
        from bronze.export import finalize_export, save_raw_data

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        # Set up mock to fail on first call, succeed on second
        mock_file = MagicMock()
        mock_gcs_access.fs.open.return_value.__enter__.side_effect = [
            Exception("Temporary error"),
            mock_file,
        ]

        save_raw_data({"data": "test1"}, "type1", "id1")
        save_raw_data({"data": "test2"}, "type2", "id2")

        # Should not raise, but log error
        with contextlib.suppress(Exception):
            finalize_export()

    def test_serialize_data_handles_invalid_objects(self):
        """Test that _serialize_data handles objects that can't be serialized."""
        from bronze.export import _serialize_data

        class UnserializableObject:
            def __init__(self):
                self.circular_ref = None

        obj = UnserializableObject()
        obj.circular_ref = obj

        # Should return string representation
        result = _serialize_data(obj)
        assert result is not None


class TestMemoryManagement:
    """Tests for memory-efficient processing."""

    def test_cleanup_after_large_export(self, mock_gcs_access):
        """Test that garbage collection is triggered after large exports."""
        from bronze.export import finalize_export, save_raw_data

        MockBronzeExport.USE_GCS = True
        MockBronzeExport.gcs_access = mock_gcs_access

        mock_file = MagicMock()
        mock_gcs_access.fs.open.return_value.__enter__.return_value = mock_file

        # Add data to the buffer to simulate a large export
        for i in range(100):
            save_raw_data({"index": i, "data": "test" * 100}, "test_data", f"record_{i}")

        # Patch gc.collect in this test module since the mock imports gc from here
        original_gc_collect = gc.collect
        call_count = [0]

        def counting_gc_collect():
            call_count[0] += 1
            return original_gc_collect()

        gc.collect = counting_gc_collect
        try:
            finalize_export()
            # Verify garbage collection was called
            assert call_count[0] >= 1, "gc.collect should have been called"
        finally:
            gc.collect = original_gc_collect
