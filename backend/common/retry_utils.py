"""Retry utilities for robust API and network operations.

This module provides standardized retry configurations using tenacity,
ensuring consistent error handling across all pipelines.

Usage Guidelines:
-----------------

1. ALWAYS use exponential backoff for external API calls
2. NEVER retry on 4xx client errors (except 429 rate limit)
3. ALWAYS set a maximum number of retries
4. Log retry attempts for debugging

Example Usage:
--------------

```python
from common.retry_utils import (
    retry_api_call,
    retry_gcs_operation,
    create_custom_retry,
    is_retryable_error,
)

# For API calls (external services)
@retry_api_call
def fetch_from_api(url: str) -> dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# For GCS operations
@retry_gcs_operation
def upload_to_gcs(bucket, blob_name, data):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

# Custom retry for specific use cases
custom_retry = create_custom_retry(
    max_attempts=5,
    min_wait=2,
    max_wait=60,
    retry_on=(ConnectionError, TimeoutError),
)

@custom_retry
def my_operation():
    pass
```

Retry Strategy Guidelines:
--------------------------

| Operation Type      | Max Attempts | Initial Wait | Max Wait | Retry On              |
|---------------------|--------------|--------------|----------|-----------------------|
| External API        | 3            | 1s           | 30s      | 5xx, network errors   |
| GCS Operations      | 5            | 1s           | 60s      | 5xx, network, timeout |
| Database Operations | 3            | 0.5s         | 10s      | Connection errors     |
| Rate Limited APIs   | 5            | 5s           | 120s     | 429 only              |

NEVER retry:
- 400 Bad Request (fix your code)
- 401 Unauthorized (fix credentials)
- 403 Forbidden (fix permissions)
- 404 Not Found (resource doesn't exist)
- 422 Unprocessable Entity (fix your data)
"""

import logging
from collections.abc import Callable

from tenacity import (
    RetryCallState,
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Exception Classification
# =============================================================================


def is_retryable_status_code(status_code: int) -> bool:
    """Determine if an HTTP status code should trigger a retry.

    Args:
        status_code: HTTP status code

    Returns:
        True if the request should be retried

    Retryable:
        - 429: Rate limited (wait and retry)
        - 500: Internal server error
        - 502: Bad gateway
        - 503: Service unavailable
        - 504: Gateway timeout

    NOT Retryable:
        - 4xx (except 429): Client errors - fix your code/data
    """
    if status_code == 429:
        return True
    return 500 <= status_code < 600


def is_retryable_error(exception: BaseException) -> bool:
    """Check if an exception should trigger a retry.

    Args:
        exception: The exception to check

    Returns:
        True if the operation should be retried
    """
    import requests

    # Network errors - always retry
    if isinstance(exception, ConnectionError | TimeoutError | OSError):
        return True

    # Requests library errors
    if isinstance(exception, requests.exceptions.RequestException):
        if (
            isinstance(exception, requests.exceptions.HTTPError)
            and exception.response is not None
        ):
            return is_retryable_status_code(exception.response.status_code)
        # Connection errors, timeouts, etc.
        return True

    return False


# =============================================================================
# Retry Decorators
# =============================================================================


def _log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempts with useful context."""
    logger.warning(
        "Retry attempt %d for %s after %.2f seconds (reason: %s)",
        retry_state.attempt_number,
        retry_state.fn.__name__ if retry_state.fn else "unknown",
        retry_state.seconds_since_start,
        retry_state.outcome.exception() if retry_state.outcome else "unknown",
    )


# Standard retry for external API calls
retry_api_call = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.DEBUG),
    reraise=True,
)
"""Retry decorator for external API calls.

Configuration:
- Max attempts: 3
- Wait: Exponential backoff (1s, 2s, 4s... up to 30s)
- Retries on: Network errors (ConnectionError, TimeoutError, OSError)

Usage:
    @retry_api_call
    def fetch_data(url):
        return requests.get(url).json()
"""


# Retry for GCS operations (more patient)
retry_gcs_operation = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.DEBUG),
    reraise=True,
)
"""Retry decorator for Google Cloud Storage operations.

Configuration:
- Max attempts: 5
- Wait: Exponential backoff (1s, 2s, 4s... up to 60s)
- Retries on: Network errors

Usage:
    @retry_gcs_operation
    def upload_file(bucket, blob_name, data):
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data)
"""


# Retry for rate-limited APIs
retry_rate_limited = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=5, min=5, max=120),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)
"""Retry decorator for rate-limited APIs.

Configuration:
- Max attempts: 5
- Wait: Exponential backoff (5s, 10s, 20s... up to 120s)
- Longer waits to respect rate limits

Usage:
    @retry_rate_limited
    def call_rate_limited_api():
        response = requests.get(url)
        if response.status_code == 429:
            raise RateLimitError()
        return response.json()
"""


def create_custom_retry(
    max_attempts: int = 3,
    min_wait: float = 1,
    max_wait: float = 30,
    retry_on: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError),
) -> Callable:
    """Create a custom retry decorator with specific parameters.

    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries (seconds)
        max_wait: Maximum wait time between retries (seconds)
        retry_on: Tuple of exception types to retry on

    Returns:
        A tenacity retry decorator

    Example:
        custom_retry = create_custom_retry(
            max_attempts=5,
            min_wait=2,
            max_wait=60,
            retry_on=(ConnectionError, TimeoutError, MyCustomError),
        )

        @custom_retry
        def my_operation():
            pass
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(retry_on),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
