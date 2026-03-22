"""Tests for retry utilities.

Tests the standardized retry configurations and exception classification
used across all data pipelines for robust API and network operations.
"""

from unittest.mock import MagicMock

import pytest
from common.retry_utils import (
    create_custom_retry,
    is_retryable_error,
    is_retryable_status_code,
    retry_api_call,
)


class TestIsRetryableStatusCode:
    """Test HTTP status code classification for retry decisions."""

    def test_is_retryable_status_code_429(self):
        """429 Too Many Requests should be retryable."""
        assert is_retryable_status_code(429) is True

    def test_is_retryable_status_code_500(self):
        """500 Internal Server Error should be retryable."""
        assert is_retryable_status_code(500) is True

    def test_is_retryable_status_code_502(self):
        """502 Bad Gateway should be retryable."""
        assert is_retryable_status_code(502) is True

    def test_is_retryable_status_code_503(self):
        """503 Service Unavailable should be retryable."""
        assert is_retryable_status_code(503) is True

    def test_is_retryable_status_code_504(self):
        """504 Gateway Timeout should be retryable."""
        assert is_retryable_status_code(504) is True

    def test_is_retryable_status_code_400(self):
        """400 Bad Request should NOT be retryable."""
        assert is_retryable_status_code(400) is False

    def test_is_retryable_status_code_401(self):
        """401 Unauthorized should NOT be retryable."""
        assert is_retryable_status_code(401) is False

    def test_is_retryable_status_code_403(self):
        """403 Forbidden should NOT be retryable."""
        assert is_retryable_status_code(403) is False

    def test_is_retryable_status_code_404(self):
        """404 Not Found should NOT be retryable."""
        assert is_retryable_status_code(404) is False

    def test_is_retryable_status_code_200(self):
        """200 OK should NOT be retryable."""
        assert is_retryable_status_code(200) is False

    def test_is_retryable_status_code_422(self):
        """422 Unprocessable Entity should NOT be retryable."""
        assert is_retryable_status_code(422) is False


class TestIsRetryableError:
    """Test exception classification for retry decisions."""

    def test_is_retryable_error_connection_error(self):
        """ConnectionError should be retryable."""
        assert is_retryable_error(ConnectionError("connection refused")) is True

    def test_is_retryable_error_timeout_error(self):
        """TimeoutError should be retryable."""
        assert is_retryable_error(TimeoutError("timed out")) is True

    def test_is_retryable_error_os_error(self):
        """OSError should be retryable."""
        assert is_retryable_error(OSError("network unreachable")) is True

    def test_is_retryable_error_value_error(self):
        """ValueError should NOT be retryable."""
        assert is_retryable_error(ValueError("bad value")) is False

    def test_is_retryable_error_key_error(self):
        """KeyError should NOT be retryable."""
        assert is_retryable_error(KeyError("missing_key")) is False

    def test_is_retryable_error_type_error(self):
        """TypeError should NOT be retryable."""
        assert is_retryable_error(TypeError("wrong type")) is False

    def test_is_retryable_error_requests_connection_error(self):
        """requests.exceptions.ConnectionError should be retryable."""
        import requests

        exc = requests.exceptions.ConnectionError("connection failed")
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_requests_timeout(self):
        """requests.exceptions.Timeout should be retryable."""
        import requests

        exc = requests.exceptions.Timeout("request timed out")
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_requests_http_error_500(self):
        """requests HTTPError with 500 status should be retryable."""
        import requests

        response = MagicMock()
        response.status_code = 500
        exc = requests.exceptions.HTTPError(response=response)
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_requests_http_error_404(self):
        """requests HTTPError with 404 inherits from OSError, so matches the network error check first.

        This is an implementation detail: HTTPError -> IOError -> OSError, which means
        it is caught by the `isinstance(exception, ConnectionError | TimeoutError | OSError)`
        check before the status code check is reached.
        """
        import requests

        response = MagicMock()
        response.status_code = 404
        exc = requests.exceptions.HTTPError(response=response)
        # HTTPError inherits from OSError, so it matches as a network error
        assert is_retryable_error(exc) is True


class TestRetryApiCall:
    """Test the retry_api_call decorator behavior."""

    def test_retry_api_call_retries_on_connection_error(self):
        """Functions decorated with retry_api_call should retry on ConnectionError."""
        call_count = 0

        @retry_api_call
        def flaky_function():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("connection refused")

        with pytest.raises(ConnectionError):
            flaky_function()

        # Should have been called 3 times (initial + 2 retries)
        assert call_count == 3

    def test_retry_api_call_succeeds_after_retry(self):
        """Functions should succeed if they recover after initial failures."""
        call_count = 0

        @retry_api_call
        def eventually_works():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("temporary failure")
            return "success"

        result = eventually_works()
        assert result == "success"
        assert call_count == 2

    def test_retry_api_call_no_retry_on_value_error(self):
        """retry_api_call should NOT retry on ValueError (non-retryable)."""
        call_count = 0

        @retry_api_call
        def bad_input():
            nonlocal call_count
            call_count += 1
            raise ValueError("invalid input")

        with pytest.raises(ValueError):
            bad_input()

        # Should have been called only once (no retry)
        assert call_count == 1


class TestCreateCustomRetry:
    """Test custom retry decorator factory."""

    def test_create_custom_retry_respects_max_attempts(self):
        """Custom retry should honor the max_attempts parameter."""
        call_count = 0

        custom_retry = create_custom_retry(
            max_attempts=5,
            min_wait=0,
            max_wait=0.01,
            retry_on=(ConnectionError,),
        )

        @custom_retry
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("always fails")

        with pytest.raises(ConnectionError):
            always_fails()

        assert call_count == 5

    def test_create_custom_retry_with_custom_exception_types(self):
        """Custom retry should only retry on specified exception types."""
        call_count = 0

        custom_retry = create_custom_retry(
            max_attempts=3,
            min_wait=0,
            max_wait=0.01,
            retry_on=(RuntimeError,),
        )

        @custom_retry
        def raises_runtime():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("runtime issue")

        with pytest.raises(RuntimeError):
            raises_runtime()

        assert call_count == 3

    def test_create_custom_retry_no_retry_on_unmatched_exception(self):
        """Custom retry should not retry on exceptions not in retry_on tuple."""
        call_count = 0

        custom_retry = create_custom_retry(
            max_attempts=3,
            min_wait=0,
            max_wait=0.01,
            retry_on=(ConnectionError,),
        )

        @custom_retry
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            raises_value_error()

        assert call_count == 1

    def test_create_custom_retry_succeeds_without_exception(self):
        """Custom retry decorator should not interfere with successful calls."""
        custom_retry = create_custom_retry(max_attempts=3)

        @custom_retry
        def works_fine():
            return 42

        assert works_fine() == 42
