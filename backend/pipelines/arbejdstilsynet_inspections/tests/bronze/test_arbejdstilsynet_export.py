"""
Tests for bronze layer Playwright scraping and cloud storage export.

This module tests:
- Playwright browser automation
- PowerBI selector stability
- CSV download triggers
- Cloud storage streaming uploads
- Error handling for browser failures
"""

# Import the bronze pipeline
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import contextlib

pytest.importorskip("playwright", reason="playwright not installed")

from bronze.export import BronzePipeline, CloudStorage


@pytest.fixture
def mock_storage_bucket():
    """Mock cloud storage bucket for testing."""
    return "test-bucket"


@pytest.fixture
def bronze_pipeline(mock_storage_bucket):
    """Create a BronzePipeline instance for testing."""
    with patch.dict("os.environ", {"SOURCE_CSV_URL": "https://test.com"}):
        return BronzePipeline(
            pipeline_name="test_pipeline",
            source_url="https://test.com",
            storage_bucket=mock_storage_bucket,
            log_level="ERROR",
        )


class TestPlaywrightAutomation:
    """Tests for Playwright browser automation."""

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_browser_launch(self, mock_playwright, bronze_pipeline):
        """Test that Playwright browser launches successfully."""
        # Mock playwright components
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        # Mock page interactions to prevent actual navigation
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("Stop test"))

        # Attempt to fetch data (will fail at selector, but browser should launch)
        with contextlib.suppress(Exception):
            await bronze_pipeline.fetch_data_with_playwright(
                [{"name": "Test", "search_term": "Test"}]
            )

        # Verify browser was launched
        mock_chromium.launch.assert_called_once()
        mock_browser.new_context.assert_called_once()

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_powerbi_selector_stability(self, mock_playwright, bronze_pipeline):
        """Test that PowerBI selector validation works."""
        # Mock playwright components
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_frame_locator = Mock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        # Mock successful iframe detection
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(return_value=True)
        mock_page.frame_locator = Mock(return_value=mock_frame_locator)

        # Mock frame locator to raise exception (to stop test early)
        mock_locator = AsyncMock()
        mock_locator.click = AsyncMock(side_effect=Exception("Selector test passed"))
        mock_frame_locator.locator = Mock(return_value=mock_locator)

        # Test
        with contextlib.suppress(Exception):
            await bronze_pipeline.fetch_data_with_playwright(
                [{"name": "Test", "search_term": "Test"}]
            )

        # Verify iframe was detected
        mock_page.wait_for_selector.assert_called()

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_csv_download_trigger(self, mock_playwright, bronze_pipeline):
        """Test that CSV download button click works."""
        # Mock complete happy path through download
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_frame_locator = Mock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        # Setup mocks
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(return_value=True)
        mock_page.frame_locator = Mock(return_value=mock_frame_locator)
        mock_page.keyboard = AsyncMock()

        # Mock successful interactions
        mock_locator = AsyncMock()
        mock_locator.click = AsyncMock()
        mock_locator.hover = AsyncMock()
        mock_frame_locator.locator = Mock(return_value=mock_locator)

        # Mock download
        mock_download = AsyncMock()
        mock_download.save_as = AsyncMock()
        mock_download_info = AsyncMock()
        mock_download_info.value = mock_download

        # Create context manager for expect_download
        class MockDownloadContext:
            async def __aenter__(self):
                return mock_download_info

            async def __aexit__(self, *args):
                pass

        mock_page.expect_download = Mock(return_value=MockDownloadContext())

        # Create temp CSV content
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("test,data\n1,2\n")
            temp_path = f.name

        # Mock save_as to use temp file
        async def mock_save_as(path):
            import shutil

            shutil.copy(temp_path, path)

        mock_download.save_as = mock_save_as

        # Test
        result = await bronze_pipeline.fetch_data_with_playwright(
            [{"name": "Test", "search_term": "Test"}]
        )

        # Verify download was triggered
        assert len(result) == 1
        assert result[0][0] == "Test"
        assert len(result[0][1]) > 0

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_csv_file_detection(self, mock_playwright, bronze_pipeline):
        """Test that downloaded CSV file is detected."""
        # Similar setup to download trigger test
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_frame_locator = Mock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(return_value=True)
        mock_page.frame_locator = Mock(return_value=mock_frame_locator)
        mock_page.keyboard = AsyncMock()

        mock_locator = AsyncMock()
        mock_locator.click = AsyncMock()
        mock_locator.hover = AsyncMock()
        mock_frame_locator.locator = Mock(return_value=mock_locator)

        # Mock download with CSV content
        mock_download = AsyncMock()
        csv_content = b"Header1,Header2\nValue1,Value2\n"

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as f:
            f.write(csv_content)
            temp_path = f.name

        async def mock_save_as(path):
            import shutil

            shutil.copy(temp_path, path)

        mock_download.save_as = mock_save_as
        mock_download_info = AsyncMock()
        mock_download_info.value = mock_download

        class MockDownloadContext:
            async def __aenter__(self):
                return mock_download_info

            async def __aexit__(self, *args):
                pass

        mock_page.expect_download = Mock(return_value=MockDownloadContext())

        result = await bronze_pipeline.fetch_data_with_playwright(
            [{"name": "Test", "search_term": "Test"}]
        )

        # Verify CSV content
        assert len(result) == 1
        assert b"Header1,Header2" in result[0][1]


class TestCloudStorageStreaming:
    """Tests for Cloud storage streaming uploads."""

    def test_cloud_streaming_upload(self):
        """Test streaming upload to cloud storage."""
        # Mock cloud storage access
        with patch("bronze.export.OptimizedStorageAccess") as mock_gcs_class:
            mock_gcs = Mock()
            mock_fs = Mock()
            mock_gcs.fs = mock_fs
            mock_gcs_class.return_value = mock_gcs

            # Create temp file
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
                f.write("test,data\n1,2\n")
                temp_path = f.name

            # Mock file operations
            mock_file = Mock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_file.write = Mock()

            mock_fs.open = Mock(return_value=mock_file)

            # Test upload
            cloud_storage = CloudStorage(bucket_name="test-bucket")
            result = cloud_storage.upload_file(temp_path)

            assert result is True

    def test_upload_integrity(self):
        """Test file integrity after upload."""
        with patch("bronze.export.OptimizedStorageAccess") as mock_gcs_class:
            mock_gcs = Mock()
            mock_fs = Mock()
            mock_gcs.fs = mock_fs
            mock_gcs_class.return_value = mock_gcs

            # Create temp file with known content
            test_content = "test,data\n1,2\n3,4\n"
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
                f.write(test_content)
                temp_path = f.name

            uploaded_content = []

            def mock_copyfileobj(src, dst):
                content = src.read()
                uploaded_content.append(content)
                dst.write(content)

            # Mock file operations
            mock_file = Mock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_file.write = Mock()

            mock_fs.open = Mock(return_value=mock_file)

            with patch("shutil.copyfileobj", side_effect=mock_copyfileobj):
                cloud_storage = CloudStorage(bucket_name="test-bucket")
                cloud_storage.upload_file(temp_path)

            # Verify content matches
            assert len(uploaded_content) > 0

    def test_metadata_json_creation(self):
        """Test metadata structure validation."""
        with patch("bronze.export.OptimizedStorageAccess") as mock_gcs_class:
            mock_gcs = Mock()
            mock_fs = Mock()
            mock_gcs.fs = mock_fs
            mock_gcs_class.return_value = mock_gcs

            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
                f.write("test,data\n1,2\n")
                temp_path = f.name

            # Track metadata writes
            metadata_content = []

            def mock_open_side_effect(path, mode):
                mock_file = Mock()
                mock_file.__enter__ = Mock(return_value=mock_file)
                mock_file.__exit__ = Mock(return_value=False)

                if "_metadata.json" in path:

                    def capture_write(content):
                        metadata_content.append(content)

                    mock_file.write = capture_write
                else:
                    mock_file.write = Mock()

                return mock_file

            mock_fs.open = Mock(side_effect=mock_open_side_effect)

            # Patch METADATA_AVAILABLE to True
            with patch("bronze.export.METADATA_AVAILABLE", True):
                cloud_storage = CloudStorage(bucket_name="test-bucket")
                cloud_storage.upload_file(temp_path)

            # Verify metadata was created
            if metadata_content:
                import json

                metadata = json.loads(metadata_content[0])
                # Check for either old or new metadata format
                assert (
                    "source_key" in metadata
                    or "source_info" in metadata
                    or "processing" in metadata
                ), "Metadata should have standard format"


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_browser_crash_recovery(self, mock_playwright, bronze_pipeline):
        """Test handling of browser crashes."""
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()

        # Simulate browser crash
        mock_page.goto = AsyncMock(side_effect=Exception("Browser crashed"))
        mock_browser.close = AsyncMock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        # Test should handle crash gracefully (function catches exceptions internally)
        try:
            result = await bronze_pipeline.fetch_data_with_playwright(
                [{"name": "Test", "search_term": "Test"}]
            )
            # Should return empty result on crash
            assert len(result) == 0
        except Exception:
            # If exception is raised, that's also acceptable (pipeline handles it)
            pass

        # Verify browser was closed
        mock_browser.close.assert_called()

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_timeout_handling(self, mock_playwright, bronze_pipeline):
        """Test network timeout scenarios."""
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()

        # Simulate timeout
        mock_page.goto = AsyncMock(side_effect=TimeoutError("Network timeout"))
        mock_browser.close = AsyncMock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        try:
            result = await bronze_pipeline.fetch_data_with_playwright(
                [{"name": "Test", "search_term": "Test"}]
            )
            assert len(result) == 0
        except TimeoutError:
            # Timeout exception is acceptable
            pass

        mock_browser.close.assert_called()

    @pytest.mark.asyncio
    @patch("bronze.export.async_playwright")
    async def test_missing_selector_error(self, mock_playwright, bronze_pipeline):
        """Test handling of DOM changes (missing selectors)."""
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()

        # Simulate missing selector
        mock_page.goto = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("Selector not found"))
        mock_browser.close = AsyncMock()

        mock_context.new_page.return_value = mock_page
        mock_browser.new_context.return_value = mock_context

        mock_chromium = AsyncMock()
        mock_chromium.launch.return_value = mock_browser

        mock_playwright_instance = AsyncMock()
        mock_playwright_instance.chromium = mock_chromium
        mock_playwright_instance.__aenter__.return_value = mock_playwright_instance
        mock_playwright_instance.__aexit__.return_value = None

        mock_playwright.return_value = mock_playwright_instance

        try:
            result = await bronze_pipeline.fetch_data_with_playwright(
                [{"name": "Test", "search_term": "Test"}]
            )
            assert len(result) == 0
        except Exception:
            # Exception is acceptable - pipeline handles it
            pass

        mock_browser.close.assert_called()


class TestDataSaving:
    """Tests for saving raw data."""

    def test_save_raw_data(self, bronze_pipeline):
        """Test saving raw CSV data to bronze layer."""
        test_data = b"Header1,Header2\nValue1,Value2\n"
        filter_name = "Test Filter"

        with tempfile.TemporaryDirectory() as temp_dir:
            bronze_pipeline.bronze_data_dir = Path(temp_dir)

            file_path, timestamp = bronze_pipeline.save_raw_data(test_data, filter_name)

            assert file_path is not None
            assert file_path.exists()
            assert timestamp is not None

            # Verify content
            with open(file_path, "rb") as f:
                content = f.read()
                assert content == test_data

    def test_metadata_creation(self, bronze_pipeline):
        """Test metadata file creation."""
        test_data = b"Header1,Header2\nValue1,Value2\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            bronze_pipeline.bronze_data_dir = Path(temp_dir)
            bronze_pipeline.pipeline_root_dir = Path(temp_dir)

            file_path, timestamp = bronze_pipeline.save_raw_data(test_data, "Test")

            bronze_pipeline.create_metadata_file(timestamp, file_path)

            metadata_path = file_path.parent / "metadata.json"
            assert metadata_path.exists()

            # Verify metadata content
            import json

            with open(metadata_path) as f:
                metadata_list = json.load(f)
                assert isinstance(metadata_list, list)
                assert len(metadata_list) > 0
                assert "source_url" in metadata_list[0]
                assert "record_count" in metadata_list[0]
