"""
Timing utility for measuring execution time of functions.
Provides decorators and context managers for timing code.
"""

import functools
import time
from typing import Any, Callable, Optional, TypeVar, cast

from unified_pipeline.util.log_util import Logger

# Type variable for preserving function signature
F = TypeVar("F", bound=Callable[..., Any])

log = Logger.get_logger()


def timed(func: Optional[F] = None, *, name: Optional[str] = None) -> Any:
    """
    Decorator to measure the execution time of a function.

    Args:
        func: The function to time
        name: Optional custom name for the timer (defaults to function name)

    Returns:
        The wrapped function with timing functionality

    Example:
        >>>
        @timed
        def my_function():
            # function code
        >>>
        @timed(name="Custom Timer")
        def another_function():
            # function code
    """

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            timer_name = name or fn.__qualname__
            start_time = time.time()
            try:
                result = fn(*args, **kwargs)
                return result
            finally:
                elapsed_time = time.time() - start_time
                log.info(f"{timer_name} executed in {elapsed_time:.4f} seconds")

        return cast(F, wrapper)

    if func is None:
        return decorator
    else:
        return decorator(func)


class Timer:
    """
    Context manager for timing code blocks.

    Args:
        name: Name of the timer for logging
        logger: Optional logger to use (defaults to logging.getLogger(__name__))

    Example:
        >>>
        with Timer("My operation"):
            # code to time
        >>>
        with Timer("Database query"):
            # database operations
    """

    def __init__(self, name: str):
        self.name = name
        self.start_time = 0.0

    def __enter__(self) -> "Timer":
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        elapsed_time = time.time() - self.start_time
        log.info(f"{self.name} completed in {elapsed_time:.4f} seconds")

    def elapsed(self) -> float:
        """Get the current elapsed time without exiting the context"""
        return time.time() - self.start_time


async def async_timed(func: Optional[F] = None, *, name: Optional[str] = None) -> Any:
    """
    Decorator to measure the execution time of an async function.

    Args:
        func: The async function to time
        name: Optional custom name for the timer (defaults to function name)

    Returns:
        The wrapped async function with timing functionality

    Example:
        >>>
        @async_timed
        async def my_async_function():
            # async function code
        >>>
        @async_timed(name="Custom Async Timer")
        async def another_async_function():
            # async function code
    """

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            timer_name = name or fn.__qualname__

            start_time = time.time()
            try:
                result = await fn(*args, **kwargs)
                return result
            finally:
                elapsed_time = time.time() - start_time
                log.info(f"{timer_name} executed in {elapsed_time:.4f} seconds")

        return cast(F, wrapper)

    if func is None:
        return decorator
    else:
        return decorator(func)


class AsyncTimer:
    """
    Context manager for timing async code blocks.

    Args:
        name: Name of the timer for logging

    Example:
        >>>
        async with AsyncTimer("My async operation"):
            # async code to time
        >>>
        async with AsyncTimer("API call"):
            # async API operations
    """

    def __init__(self, name: str):
        self.name = name
        self.start_time = 0.0

    async def __aenter__(self) -> "AsyncTimer":
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        elapsed_time = time.time() - self.start_time
        log.info(f"{self.name} completed in {elapsed_time:.4f} seconds")

    def elapsed(self) -> float:
        """Get the current elapsed time without exiting the context"""
        return time.time() - self.start_time
