from __future__ import annotations

import os
import sys
from enum import Enum
from typing import Optional

import loguru
from simple_singleton import Singleton


class LogLevel(Enum):
    """
    Class containing helper constants for log levels.
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
    Provides a logger with a default format and level. Default level is INFO.
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
    LOG = None
    DEFAULT_LOG = "INFO"

    def __init__(self) -> None:
        pass

    @classmethod
    def _get_alias_log_level(cls, log_level: str) -> str:
        """
        Get the alias log level for the given log level.
        :param log_level: The log level.
        :return: The alias log level.
        """
        return cls._log_level_aliases[log_level]

    @classmethod
    def get_logger(cls, level: Optional[str] = None) -> loguru.Logger:
        """
        Returns a logger with a default format and level. Default level is INFO.
        :param level: The level of the logger. Default is INFO.
        """
        if cls.LOG is None:
            if level is None:
                level = cls._get_alias_log_level(os.environ.get("LOG_LEVEL", cls.DEFAULT_LOG))
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
