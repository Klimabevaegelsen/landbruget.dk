"""
Logging utilities for the BBR Buildings Pipeline.
"""

from common.logging_utils import setup_pipeline_logger


def setup_logger(name: str = "bbr_buildings", level: str = "INFO"):
    """
    Set up a logger with the specified name and level.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance
    """
    return setup_pipeline_logger(name, level=level)
