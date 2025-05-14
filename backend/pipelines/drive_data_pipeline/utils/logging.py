"""Logging utilities for Google Drive Data Pipeline."""

import contextvars
import json
import sys
from typing import Any, Dict, Optional

from loguru import logger
from loguru._defaults import LOGURU_FORMAT


# Context variables to store context data like file_id, folder, etc.
context_data = contextvars.ContextVar("context_data", default={})


def setup_logging(log_level: str = "INFO") -> None:
    """Set up logging with the specified log level.

    Args:
        log_level: The log level to use (DEBUG, INFO, WARNING, ERROR)
    """
    # Remove default handler
    logger.remove()

    # Add a handler with custom format
    logger.add(
        sys.stderr,
        format=_format_record,
        level=log_level,
        colorize=True,
        diagnose=True,
    )
    logger.info(f"Logging initialized at level {log_level}")


def _format_record(record: Dict[str, Any]) -> str:
    """Format log records with additional context.

    Args:
        record: The log record to format

    Returns:
        Formatted log record as string
    """
    # Get context data
    ctx_data = context_data.get()

    # Add context to record extras if there is context data
    if ctx_data:
        for key, value in ctx_data.items():
            record["extra"][key] = value

    # Use standard format if message is not a dict
    if isinstance(record["message"], str):
        formatted = LOGURU_FORMAT

    # If message is a dict, add it to the extras
    elif isinstance(record["message"], dict):
        record["extra"].update(record["message"])
        formatted = "{time} | {level} | {extra}"
    else:
        formatted = LOGURU_FORMAT

    return formatted


def get_logger():
    """Get the configured logger.

    Returns:
        The configured logger
    """
    return logger


def set_context(**kwargs) -> None:
    """Set context data for log messages.

    Args:
        **kwargs: Key-value pairs to add to the context
    """
    # Get current context
    current_ctx = context_data.get()
    
    # Update with new values
    current_ctx.update(kwargs)
    
    # Set updated context
    context_data.set(current_ctx)


def clear_context() -> None:
    """Clear all context data."""
    context_data.set({}) 