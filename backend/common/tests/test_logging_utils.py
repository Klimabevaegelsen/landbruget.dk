"""Tests for logging utilities.

Tests the standardized logging configuration and context managers
used across all data pipelines.
"""

import logging

from common.logging_utils import (
    PipelineLogger,
    StageLogger,
    get_pipeline_logger,
    setup_pipeline_logger,
)


class TestSetupPipelineLogger:
    """Test setup_pipeline_logger function."""

    def test_setup_pipeline_logger_returns_logger(self):
        """setup_pipeline_logger should return a Logger with the correct name."""
        logger = setup_pipeline_logger("test_pipeline", console_output=False)

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_pipeline"

    def test_setup_pipeline_logger_sets_level(self):
        """setup_pipeline_logger should set the correct log level."""
        logger = setup_pipeline_logger("level_test", level="DEBUG", console_output=False)
        assert logger.level == logging.DEBUG

        logger2 = setup_pipeline_logger("level_test_warn", level="WARNING", console_output=False)
        assert logger2.level == logging.WARNING

    def test_setup_pipeline_logger_with_file(self, tmp_path):
        """setup_pipeline_logger should create a log file when log_file is specified."""
        log_file = tmp_path / "test_pipeline.log"

        logger = setup_pipeline_logger(
            "file_test",
            log_file=str(log_file),
            console_output=False,
        )

        logger.info("Test log message")

        # Force flush
        for handler in logger.handlers:
            handler.flush()

        assert log_file.exists()
        content = log_file.read_text()
        assert "Test log message" in content

    def test_setup_pipeline_logger_creates_nested_log_dir(self, tmp_path):
        """setup_pipeline_logger should create parent directories for the log file."""
        log_file = tmp_path / "nested" / "dir" / "pipeline.log"

        logger = setup_pipeline_logger(
            "nested_dir_test",
            log_file=str(log_file),
            console_output=False,
        )

        logger.info("Nested directory test")

        for handler in logger.handlers:
            handler.flush()

        assert log_file.exists()

    def test_setup_pipeline_logger_no_propagation(self):
        """Logger should not propagate to root logger to avoid duplicate messages."""
        logger = setup_pipeline_logger("no_propagate_test", console_output=False)
        assert logger.propagate is False

    def test_setup_pipeline_logger_clears_existing_handlers(self):
        """Calling setup_pipeline_logger twice should not duplicate handlers."""
        logger1 = setup_pipeline_logger("dup_test", console_output=True)
        handler_count_1 = len(logger1.handlers)

        logger2 = setup_pipeline_logger("dup_test", console_output=True)
        handler_count_2 = len(logger2.handlers)

        assert handler_count_1 == handler_count_2

    def test_setup_pipeline_logger_without_timestamp(self):
        """Logger without timestamp should use a simpler format."""
        logger = setup_pipeline_logger(
            "no_ts_test",
            include_timestamp=False,
            console_output=False,
        )

        # Verify logger is usable (does not raise)
        assert isinstance(logger, logging.Logger)


class TestGetPipelineLogger:
    """Test get_pipeline_logger function."""

    def test_get_pipeline_logger_creates_logger(self, tmp_path, monkeypatch):
        """get_pipeline_logger should create a logger with the pipeline. prefix."""
        monkeypatch.setenv("LOG_DIR", str(tmp_path))

        logger = get_pipeline_logger("test_auto")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "pipeline.test_auto"
        assert len(logger.handlers) > 0

    def test_get_pipeline_logger_returns_same_logger_on_second_call(self, tmp_path, monkeypatch):
        """Calling get_pipeline_logger twice with the same name should return the same logger."""
        monkeypatch.setenv("LOG_DIR", str(tmp_path))

        logger1 = get_pipeline_logger("same_name_test")
        logger2 = get_pipeline_logger("same_name_test")

        assert logger1 is logger2


class TestPipelineLoggerContextManager:
    """Test PipelineLogger context manager."""

    def test_pipeline_logger_context_manager_enter_exit(self, tmp_path, monkeypatch):
        """PipelineLogger should work as a context manager, yielding a logger."""
        monkeypatch.setenv("LOG_DIR", str(tmp_path))

        with PipelineLogger("ctx_test", version="2.0.0") as logger:
            assert isinstance(logger, logging.Logger)
            logger.info("Inside pipeline context")

    def test_pipeline_logger_with_custom_logger(self):
        """PipelineLogger should accept a custom logger instance."""
        custom_logger = setup_pipeline_logger("custom_ctx", console_output=False)

        with PipelineLogger("custom_pipeline", logger=custom_logger) as logger:
            assert logger is custom_logger

    def test_pipeline_logger_sets_start_time(self, tmp_path, monkeypatch):
        """PipelineLogger should set start_time on __enter__."""
        monkeypatch.setenv("LOG_DIR", str(tmp_path))

        pl = PipelineLogger("timing_test")
        assert pl.start_time is None

        with pl as _logger:
            assert pl.start_time is not None


class TestStageLoggerContextManager:
    """Test StageLogger context manager."""

    def test_stage_logger_context_manager(self):
        """StageLogger should work as a context manager, yielding self."""
        logger = setup_pipeline_logger("stage_parent", console_output=False)

        with StageLogger("bronze", logger) as stage:
            assert isinstance(stage, StageLogger)
            assert stage.stage_name == "bronze"
            assert stage.start_time is not None

    def test_stage_logger_log_progress(self):
        """log_progress should format a message through the underlying logger."""
        logger = setup_pipeline_logger("progress_test", console_output=False)

        with StageLogger("silver", logger) as stage:
            # Should not raise
            stage.log_progress("Processing records")

    def test_stage_logger_log_progress_with_record_count(self, tmp_path):
        """log_progress with records_processed should include the count in the message."""
        log_file = tmp_path / "progress.log"
        logger = setup_pipeline_logger(
            "progress_count_test",
            log_file=str(log_file),
            console_output=False,
        )

        with StageLogger("gold", logger) as stage:
            stage.log_progress("Uploaded data", records_processed=1500)

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text()
        assert "1,500 records" in content
        assert "Uploaded data" in content
