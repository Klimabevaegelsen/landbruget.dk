"""
Logging utility module that provides standardized logging configuration.

This module offers a simple interface for creating and managing loggers with
consistent formatting across the application. It uses the loguru library
and implements a singleton pattern to ensure only one logger instance exists.
"""

from __future__ import annotations

import os
import sys
from enum import Enum
from typing import Optional

import loguru
from simple_singleton import Singleton


class LogLevel(Enum):
    """
    Enumeration of standard log levels used throughout the application.

    This class provides constants for various logging levels to ensure
    consistency when specifying log verbosity in different parts of the code.

    Attributes:
        TRACE: Detailed information, typically of interest only when diagnosing problems
        DEBUG: Detailed information on the flow through the system
        INFO: Confirmation that things are working as expected
        WARN: Indication that something unexpected happened, or may happen in the near future
        ERROR: Due to a more serious problem, the software has not been able to perform a function
        FATAL: Severe error events that might cause the application to terminate
        OFF: Special level used to disable logging
    """

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"
    OFF = "OFF"


class Logger(metaclass=Singleton):
    """
    Singleton logger class that provides consistent logging configuration.

    This class ensures that only one logger instance is created throughout the
    application, maintaining consistent logging behavior. It configures loguru
    with appropriate formatting and handles log level management.

    Attributes:
        _log_level_aliases (dict): Mapping between application log levels and loguru log levels
        DEFAULT_LOG_DIR (str): Default directory where log files will be stored
        LOG (Optional[loguru.Logger]): The singleton logger instance
        DEFAULT_LOG (str): Default log level if none specified
    """

    _log_level_aliases: dict[str, str] = {
        "TRACE": "TRACE",
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARN": "WARNING",
        "ERROR": "ERROR",
        "FATAL": "CRITICAL",
        "OFF": "OFF",
    }

    DEFAULT_LOG_DIR = "/tmp/unified_pipeline/"
    LOG: Optional[loguru.Logger] = None
    DEFAULT_LOG = "INFO"

    def __init__(self) -> None:
        """
        Initialize the Logger instance.

        Due to the singleton pattern, this will only be called once regardless
        of how many times the Logger class is instantiated.
        """
        pass

    @classmethod
    def _get_alias_log_level(cls, log_level: str) -> str:
        """
        Convert application log level to its corresponding loguru log level.

        This internal method translates between the application's log level constants
        and the corresponding log levels recognized by the loguru library.

        Args:
            log_level (str): The application log level to convert

        Returns:
            str: The corresponding loguru log level

        Example:
            >>> Logger._get_alias_log_level("WARN")
            'WARNING'
        """
        return cls._log_level_aliases[log_level]

    @classmethod
    def get_logger(cls, level: Optional[str] = None) -> loguru.Logger:
        """
        Get or create a configured logger instance with the specified log level.

        Creates a singleton logger instance if it doesn't exist yet, or returns the
        existing instance. The logger outputs to both stderr and a log file with
        formatted timestamps, log levels, thread names, and source modules.

        Args:
            level (Optional[str]): Log level to use. If None, uses the LOG_LEVEL
                                  environment variable or defaults to INFO.

        Returns:
            loguru.Logger: Configured logger instance

        Example:
            >>> logger = Logger.get_logger("DEBUG")
            >>> logger.debug("This is a debug message")
        """
        if cls.LOG is None:
            if level is None:
                level = cls._get_alias_log_level(
                    os.environ.get("LOG_LEVEL", cls.DEFAULT_LOG).upper()
                )
            else:
                level = level.upper()
            log_dir = os.environ.get("LOG_DIR", cls.DEFAULT_LOG_DIR)
            cls.LOG = loguru.logger
            cls.LOG.remove()
            cls.LOG.add(
                sys.stderr,
                format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> "
                "| {thread.name: <28} | <cyan>{name}</cyan> - {message}",
                level=level,
            )
            cls.LOG.add(
                log_dir + "/log_{time}.log",
                format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> "
                "| {thread.name: <28} | <cyan>{name}</cyan> - {message}",
                level=level,
            )
        return cls.LOG
