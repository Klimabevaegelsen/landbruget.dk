"""
Shared logging utilities for all pipelines.

This module provides standardized logging configuration and utilities
to replace the inconsistent logging patterns found across pipelines.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_pipeline_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[str] = None,
    console_output: bool = True,
    include_timestamp: bool = True,
) -> logging.Logger:
    """
    Set up standardized logging for pipeline operations.
    
    This replaces the inconsistent logging setups found across different pipelines
    with a unified, configurable approach.
    
    Args:
        name: Logger name (typically the pipeline name)
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file path for log output
        console_output: Whether to output to console
        include_timestamp: Whether to include timestamp in format
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Clear any existing handlers to avoid duplication
    logger.handlers.clear()
    
    # Set level
    logger.setLevel(getattr(logging, level.upper()))
    
    # Create formatter
    if include_timestamp:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    else:
        formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_pipeline_logger(pipeline_name: str) -> logging.Logger:
    """
    Get or create a standardized logger for a pipeline.
    
    This function provides a simple way to get a consistently configured
    logger for any pipeline without worrying about setup details.
    
    Args:
        pipeline_name: Name of the pipeline
        
    Returns:
        Configured logger instance
    """
    # Check if logger already exists
    logger = logging.getLogger(f"pipeline.{pipeline_name}")
    
    if not logger.handlers:
        # Set up new logger with standard configuration
        log_level = os.getenv("LOG_LEVEL", "INFO")
        log_dir = os.getenv("LOG_DIR", "logs")
        
        # Create log file path
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = Path(log_dir) / f"{pipeline_name}_{timestamp}.log"
        
        logger = setup_pipeline_logger(
            name=f"pipeline.{pipeline_name}",
            level=log_level,
            log_file=str(log_file),
            console_output=True,
        )
    
    return logger


def log_pipeline_start(logger: logging.Logger, pipeline_name: str, version: str = "1.0.0"):
    """Log standardized pipeline start message."""
    logger.info("=" * 60)
    logger.info(f"🚀 Starting {pipeline_name} Pipeline (v{version})")
    logger.info(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def log_pipeline_end(
    logger: logging.Logger, 
    pipeline_name: str, 
    start_time: datetime, 
    success: bool = True
):
    """Log standardized pipeline completion message."""
    end_time = datetime.now()
    duration = end_time - start_time
    
    status = "✅ COMPLETED" if success else "❌ FAILED"
    
    logger.info("=" * 60)
    logger.info(f"{status} {pipeline_name} Pipeline")
    logger.info(f"⏰ End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"⏱️  Total duration: {duration}")
    logger.info("=" * 60)


def log_stage_start(logger: logging.Logger, stage_name: str):
    """Log standardized stage start message."""
    logger.info("-" * 40)
    logger.info(f"🔄 Starting {stage_name} stage")
    logger.info("-" * 40)


def log_stage_end(
    logger: logging.Logger, 
    stage_name: str, 
    start_time: datetime, 
    records_processed: Optional[int] = None,
    success: bool = True
):
    """Log standardized stage completion message."""
    duration = datetime.now() - start_time
    status = "✅" if success else "❌"
    
    message = f"{status} {stage_name} completed in {duration}"
    if records_processed is not None:
        message += f" ({records_processed:,} records)"
        
    logger.info(message)


class PipelineLogger:
    """
    Context manager for pipeline logging that provides structured logging
    with automatic start/end messages and duration tracking.
    """
    
    def __init__(
        self, 
        pipeline_name: str, 
        version: str = "1.0.0", 
        logger: Optional[logging.Logger] = None
    ):
        self.pipeline_name = pipeline_name
        self.version = version
        self.logger = logger or get_pipeline_logger(pipeline_name)
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        log_pipeline_start(self.logger, self.pipeline_name, self.version)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        log_pipeline_end(self.logger, self.pipeline_name, self.start_time, success)


class StageLogger:
    """Context manager for stage logging within pipelines."""
    
    def __init__(self, stage_name: str, logger: logging.Logger):
        self.stage_name = stage_name
        self.logger = logger
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        log_stage_start(self.logger, self.stage_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        log_stage_end(self.logger, self.stage_name, self.start_time, success=success)
    
    def log_progress(self, message: str, records_processed: Optional[int] = None):
        """Log progress message with optional record count."""
        if records_processed is not None:
            message += f" ({records_processed:,} records)"
        self.logger.info(f"  {message}")


# Example usage patterns to replace inconsistent logging across pipelines:

def example_usage():
    """
    Example usage patterns that replace the inconsistent logging
    found across different pipelines.
    """
    
    # Pattern 1: Simple pipeline logging
    with PipelineLogger("bmd_scraper", "1.2.0") as logger:
        logger.info("Processing BMD data...")
        
        with StageLogger("bronze", logger) as bronze_stage:
            bronze_stage.log_progress("Fetching data from web portal")
            # ... bronze processing ...
            bronze_stage.log_progress("Data fetched successfully", records_processed=1500)
        
        with StageLogger("silver", logger) as silver_stage:
            silver_stage.log_progress("Transforming data")
            # ... silver processing ...
            silver_stage.log_progress("Data transformed", records_processed=1450)
    
    # Pattern 2: Manual logger setup for more control
    logger = get_pipeline_logger("chr_pipeline")
    logger.info("Starting CHR data processing")
    
    # Pattern 3: Custom logger configuration
    custom_logger = setup_pipeline_logger(
        name="custom_pipeline",
        level="DEBUG",
        log_file="/var/log/pipelines/custom.log",
        console_output=False,
    )
    custom_logger.debug("Debug message for custom pipeline")