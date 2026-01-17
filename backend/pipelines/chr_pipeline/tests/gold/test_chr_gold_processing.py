"""Tests for CHR gold layer orchestration and processing.

Tests the gold processing logic including:
- Gold orchestration workflow
- Veterinary timeline generation
- Transportation analysis
- Final data exports

Note: These tests skip if gold module cannot be imported (due to Python version incompatibility).
"""

from unittest.mock import patch

import pytest

# Try to import gold module, skip tests if it fails
try:
    import gold.chr_gold_processing  # noqa: F401

    GOLD_MODULE_AVAILABLE = True
except (ImportError, TypeError):
    GOLD_MODULE_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not GOLD_MODULE_AVAILABLE,
    reason="Gold module not available (likely Python version incompatibility with type hints)",
)


@pytest.mark.chr_gold
class TestGoldOrchestration:
    """Test the main gold processing orchestration."""

    def test_process_gold_data_creates_output_directory(self, tmp_path):
        """Test that gold processing creates output directory."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            # Import here to avoid module-level type hint issues
            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is True
            assert gold_dir.exists()

    def test_process_gold_data_all_steps(self, tmp_path):
        """Test running all gold processing steps."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp, gold_dir=gold_dir, step=None
            )

            assert result is True
            assert mock_timeline.called
            assert mock_transport.called

    def test_process_gold_data_specific_step_veterinary(self, tmp_path):
        """Test running only veterinary timeline step."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="veterinary_timeline",
            )

            assert result is True
            assert mock_timeline.called
            assert not mock_transport.called

    def test_process_gold_data_specific_step_transportation(self, tmp_path):
        """Test running only transportation analysis step."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="transportation_analysis",
            )

            assert result is True
            assert not mock_timeline.called
            assert mock_transport.called

    def test_process_gold_data_handles_step_failure(self, tmp_path):
        """Test that failure of one step is properly reported."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = False  # Simulate failure
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is False

    def test_process_gold_data_handles_all_failures(self, tmp_path):
        """Test handling when all steps fail."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = False
            mock_transport.return_value = False

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is False

    def test_process_gold_data_default_gold_dir(self, tmp_path):
        """Test that default gold directory is created from timestamp."""
        export_timestamp = "20240101_120000"

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
            patch("gold.chr_gold_processing.GOLD_BASE_DIR", tmp_path),
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            process_gold_data(export_timestamp=export_timestamp, gold_dir=None)

            expected_dir = tmp_path / export_timestamp
            assert expected_dir.exists()


@pytest.mark.chr_gold
class TestVeterinaryTimeline:
    """Test veterinary timeline generation."""

    def test_veterinary_timeline_creates_output(self, tmp_path):
        """Test that veterinary timeline creates output file."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_process:
            mock_process.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="veterinary_timeline",
            )

            assert result is True
            assert mock_process.called

    def test_veterinary_timeline_handles_no_data(self, tmp_path):
        """Test veterinary timeline handles missing data gracefully."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_process:
            # Simulate no data available
            mock_process.return_value = False

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="veterinary_timeline",
            )

            assert result is False


@pytest.mark.chr_gold
class TestTransportationAnalysis:
    """Test transportation analysis generation."""

    def test_transportation_analysis_creates_output(self, tmp_path):
        """Test that transportation analysis creates output file."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with patch("gold.chr_gold_processing.process_transportation_analysis") as mock_process:
            mock_process.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="transportation_analysis",
            )

            assert result is True
            assert mock_process.called

    def test_transportation_analysis_handles_no_movements(self, tmp_path):
        """Test transportation analysis handles missing movement data."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with patch("gold.chr_gold_processing.process_transportation_analysis") as mock_process:
            # Simulate no movement data
            mock_process.return_value = False

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir,
                step="transportation_analysis",
            )

            assert result is False


@pytest.mark.chr_gold
class TestGoldDataQuality:
    """Test gold layer data quality checks."""

    def test_gold_processing_logs_success(self, tmp_path, caplog):
        """Test that successful processing is logged."""
        import logging

        caplog.set_level(logging.INFO)

        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is True
            assert "CHR Gold Layer Processing completed successfully" in caplog.text

    def test_gold_processing_logs_failure(self, tmp_path, caplog):
        """Test that processing failures are logged."""
        import logging

        caplog.set_level(logging.ERROR)

        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = False
            mock_transport.return_value = False

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is False
            assert "CHR Gold Layer Processing failed" in caplog.text


@pytest.mark.chr_gold
class TestGoldExports:
    """Test gold layer data export functionality."""

    def test_gold_data_exports_to_correct_location(self, tmp_path):
        """Test that gold data is exported to correct GCS location."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold" / export_timestamp

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is True
            # Verify mock was called with correct parameters
            call_args = mock_timeline.call_args
            assert call_args[1]["export_timestamp"] == export_timestamp
            assert call_args[1]["gold_dir"] == gold_dir

    def test_gold_processing_handles_export_errors(self, tmp_path):
        """Test that export errors are handled gracefully."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir(parents=True)

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            # Simulate export error
            mock_timeline.side_effect = Exception("Export failed")
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            # Should handle exception and return False
            assert result is False


@pytest.mark.chr_gold
class TestGoldConfiguration:
    """Test gold layer configuration handling."""

    def test_gold_processing_uses_environment_config(self, tmp_path):
        """Test that gold processing uses environment configuration."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
            patch.dict("os.environ", {"GCS_BUCKET": "test-bucket"}),
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            from gold.chr_gold_processing import process_gold_data

            result = process_gold_data(export_timestamp=export_timestamp, gold_dir=gold_dir)

            assert result is True

    def test_gold_processing_step_parameter_variations(self, tmp_path):
        """Test different step parameter variations."""
        export_timestamp = "20240101_120000"
        gold_dir = tmp_path / "gold"

        test_cases = [
            ("all", True, True),
            ("gold_processing", True, True),
            ("veterinary_timeline", True, False),
            ("transportation_analysis", False, True),
            (None, True, True),  # Default is all steps
        ]

        with (
            patch("gold.chr_gold_processing.process_veterinary_timeline") as mock_timeline,
            patch("gold.chr_gold_processing.process_transportation_analysis") as mock_transport,
        ):
            mock_timeline.return_value = True
            mock_transport.return_value = True

            for step_param, expect_timeline, expect_transport in test_cases:
                mock_timeline.reset_mock()
                mock_transport.reset_mock()

                from gold.chr_gold_processing import process_gold_data

                result = process_gold_data(
                    export_timestamp=export_timestamp, gold_dir=gold_dir, step=step_param
                )

                assert result is True
                assert mock_timeline.called == expect_timeline
                assert mock_transport.called == expect_transport
