"""Logging configuration for the pipeline."""

import logging
import sys
from typing import Any

# Create a custom logger
logger = logging.getLogger("drive_data_pipeline")

# Define ANSI color codes for different log levels and components
COLORS = {
    # Log levels
    'DEBUG': '\033[36m',     # Cyan
    'INFO': '\033[32m',      # Green
    'WARNING': '\033[33m',   # Yellow
    'ERROR': '\033[31m',     # Red
    'CRITICAL': '\033[41m',  # Red background
    
    # Other log components
    'DATETIME': '\033[34m',  # Blue
    'MODULE': '\033[35m',    # Magenta
    
    'RESET': '\033[0m'       # Reset to default
}


class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to log components."""

    def format(self, record):
        """Format the log record with colors."""
        # Add color to levelname
        levelname = record.levelname
        if levelname in COLORS:
            colored = f"{COLORS[levelname]}{levelname}{COLORS['RESET']}"
            record.levelname = colored
            
        # Store the original format
        original_fmt = self._style._fmt
        
        # Apply colors to other parts of the format string
        colored_fmt = original_fmt.replace(
            "%(asctime)s", 
            f"{COLORS['DATETIME']}%(asctime)s{COLORS['RESET']}"
        )
        
        # Color for module:function:lineno
        module_part = "%(name)s:%(funcName)s:%(lineno)d"
        colored_module = f"{COLORS['MODULE']}{module_part}{COLORS['RESET']}"
        colored_fmt = colored_fmt.replace(module_part, colored_module)
        
        # Set the colored format
        self._style._fmt = colored_fmt
        
        # Format the record
        result = super().format(record)
        
        # Restore the original format
        self._style._fmt = original_fmt
        
        return result


def get_logger() -> logging.Logger:
    """Get the configured logger.

    Returns:
        Logger instance
    """
    return logger


def set_context(**kwargs: Any) -> None:
    """Set context for logging.

    Args:
        **kwargs: Key-value pairs to add to the logging context
    """
    # This is a placeholder for more sophisticated context handling
    # In a production system, this could use something like contextvars
    # or logging adapters
    pass


def setup_logging(log_level: str = "INFO") -> None:
    """Set up logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Set level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Create formatter - Remove the newline character at the end
    format_str = (
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d"
        " - %(message)s"
    )
    # Use a standard time format without microseconds
    time_format = "%Y-%m-%d %H:%M:%S"
    formatter = ColoredFormatter(format_str, time_format)
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.handlers = []  # Clear any existing handlers
    logger.addHandler(console_handler)
    logger.propagate = False  # Prevent duplicate logs

    logger.info(f"Logging initialized at level {log_level}")


def log_exception(
    exception: Exception, 
    level: str = "ERROR", 
    context: dict[str, Any] | None = None
) -> None:
    """Log an exception with context.

    Args:
        exception: Exception to log
        level: Logging level (ERROR or CRITICAL)
        context: Additional context information
    """
    log_func = logger.error if level == "ERROR" else logger.critical
    message = f"Exception: {type(exception).__name__}: {str(exception)}"

    if context:
        context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
        message = f"{message} (Context: {context_str})"

    log_func(message, exc_info=True) 