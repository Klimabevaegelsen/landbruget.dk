"""Error handling utilities for Google Drive Data Pipeline."""

import functools
from collections.abc import Callable
from typing import Any, TypeVar

from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .logging import get_logger

# Type variable for function return type
T = TypeVar("T")

# Get logger
logger = get_logger()


def retry_with_exponential_backoff(
    max_attempts: int = 5,
    retry_exceptions: type[Exception] | list[type[Exception]] = Exception,
    min_wait_seconds: float = 1,
    max_wait_seconds: float = 60,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for retrying a function with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        retry_exceptions: Exception type(s) to retry on
        min_wait_seconds: Minimum wait time between retries in seconds
        max_wait_seconds: Maximum wait time between retries in seconds

    Returns:
        Decorator function
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retry_decorator = retry(
                retry=retry_if_exception_type(retry_exceptions),
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=1, min=min_wait_seconds, max=max_wait_seconds),
                reraise=True,
                before_sleep=lambda retry_state: logger.warning(
                    f"Retrying {func.__name__} (attempt {retry_state.attempt_number}/{max_attempts}) "
                    f"after error: {retry_state.outcome.exception()}"
                ),
            )
            
            try:
                return retry_decorator(func)(*args, **kwargs)
            except RetryError as e:
                logger.error(
                    f"Failed all {max_attempts} attempts to execute {func.__name__}"
                )
                if e.last_attempt.exception():
                    raise e.last_attempt.exception()
                raise e

        return wrapper

    return decorator


class GoogleDriveAPIError(Exception):
    """Error during Google Drive API operations."""

    pass


class FileDownloadError(Exception):
    """Error during file download operations."""

    pass


class FileProcessingError(Exception):
    """Error during file processing operations."""

    pass


class StorageError(Exception):
    """Error during storage operations."""

    pass 