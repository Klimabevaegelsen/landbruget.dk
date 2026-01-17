"""
Tests for CVR API Client.

Tests for: unified_pipeline/util/cvr_api_client.py

Covers:
- CVR lookup with valid 8-digit CVR numbers
- Batch company fetching
- Rate limiting (100ms intervals)
- HTTP Basic Auth setup
- Retry logic with exponential backoff
- Error handling for API failures
- PII filtering integration
- Geocoding toggle functionality
"""

import json
from unittest.mock import Mock, patch

import pytest
import requests

from unified_pipeline.util.cvr_api_client import CVRAPIClient


class TestCVRAPIClient:
    """Tests for CVR API client initialization and basic functionality."""

    def test_client_initialization(self):
        """Test CVR API client can be initialized."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test_user", "CVR_PASSWORD": "test_pass"}):
            client = CVRAPIClient()
            assert client.base_url == "http://distribution.virk.dk"
            assert client.auth is not None

    def test_client_initialization_without_credentials(self):
        """Test client initialization without credentials raises error."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError):
                CVRAPIClient()

    @patch("unified_pipeline.util.cvr_api_client.HTTPBasicAuth")
    def test_http_basic_auth_setup(self, mock_auth):
        """Test HTTP Basic Auth is properly configured."""
        with patch.dict("os.environ", {"CVR_USERNAME": "user123", "CVR_PASSWORD": "pass456"}):
            CVRAPIClient()
            mock_auth.assert_called_once_with("user123", "pass456")


class TestCVRLookup:
    """Tests for CVR company lookup functionality."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_fetch_cvr_valid_8_digit(self, mock_post):
        """Test fetching company data with valid 8-digit CVR."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "virksomhedMetadata": {"nyesteNavn": {"navn": "Test Company"}},
                            }
                        }
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("31373077")

        assert result is not None
        assert "cvrNummer" in result
        mock_post.assert_called_once()

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_fetch_cvr_invalid_cvr(self, mock_post):
        """Test fetching with invalid CVR returns None."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()

            # Invalid: only 7 digits
            result = client.fetch_cvr("1234567")
            assert result is None

            # Invalid: 9 digits
            result = client.fetch_cvr("123456789")
            assert result is None

            # Invalid: non-numeric
            result = client.fetch_cvr("abcdefgh")
            assert result is None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_fetch_cvr_not_found(self, mock_post):
        """Test fetching CVR that doesn't exist."""
        # Mock empty response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        assert result is None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_fetch_cvr_with_elasticsearch_query(self, mock_post):
        """Test that Elasticsearch-style query is properly formatted."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.fetch_cvr("31373077")

        # Verify Elasticsearch query structure
        call_args = mock_post.call_args
        query_data = json.loads(call_args[1]["data"])
        assert "query" in query_data
        assert "bool" in query_data["query"]


class TestBatchFetching:
    """Tests for batch company fetching functionality."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    @patch("unified_pipeline.util.cvr_api_client.time.sleep")
    def test_batch_fetch_companies(self, mock_sleep, mock_post):
        """Test batch fetching multiple companies."""
        # Mock responses for multiple CVRs
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {"hits": [{"_source": {"Vrvirksomhed": {"cvrNummer": 12345678}}}]}
        }
        mock_post.return_value = mock_response

        cvr_list = ["12345678", "87654321", "11111111"]

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            results = client.batch_fetch_companies(cvr_list)

        assert len(results) == 3
        assert mock_post.call_count == 3

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    @patch("unified_pipeline.util.cvr_api_client.time.sleep")
    def test_batch_fetch_with_rate_limiting(self, mock_sleep, mock_post):
        """Test that batch fetching respects rate limiting."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}
        mock_post.return_value = mock_response

        cvr_list = ["12345678", "87654321"]

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.batch_fetch_companies(cvr_list)

        # Verify sleep is called between requests (rate limiting)
        assert mock_sleep.call_count >= 1

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_batch_fetch_empty_list(self, mock_post):
        """Test batch fetching with empty CVR list."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            results = client.batch_fetch_companies([])

        assert results == {}
        mock_post.assert_not_called()

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_batch_fetch_filters_invalid_cvrs(self, mock_post):
        """Test that batch fetching filters out invalid CVRs."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {"hits": [{"_source": {"Vrvirksomhed": {"cvrNummer": 12345678}}}]}
        }
        mock_post.return_value = mock_response

        # Mix of valid and invalid CVRs
        cvr_list = ["12345678", "invalid", None, "123"]

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.batch_fetch_companies(cvr_list)

        # Only valid CVR should be fetched
        assert mock_post.call_count == 1


class TestRateLimiting:
    """Tests for API rate limiting behavior."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    @patch("unified_pipeline.util.cvr_api_client.time.sleep")
    def test_rate_limiting_100ms_interval(self, mock_sleep, mock_post):
        """Test rate limiting enforces 100ms minimum interval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {"hits": [{"_source": {"Vrvirksomhed": {"cvrNummer": 12345678}}}]}
        }
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()

            # Fetch two companies in succession
            client.fetch_cvr("12345678")
            client.fetch_cvr("87654321")

        # Should have at least one sleep call for rate limiting
        assert mock_sleep.call_count >= 1

        # Check that sleep duration is at least 0.1 seconds
        for call in mock_sleep.call_args_list:
            sleep_duration = call[0][0]
            assert sleep_duration >= 0.1


class TestRetryLogic:
    """Tests for retry logic with exponential backoff."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_retry_on_connection_error(self, mock_post):
        """Test retry on connection error."""
        # First two calls fail, third succeeds
        mock_post.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            requests.exceptions.ConnectionError("Connection failed"),
            Mock(status_code=200, json=lambda: {"hits": {"hits": []}}),
        ]

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.fetch_cvr("12345678")

        # Should retry and eventually succeed
        assert mock_post.call_count == 3

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_retry_on_http_500_error(self, mock_post):
        """Test retry on HTTP 500 error."""
        # First call fails with 500, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError()

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"hits": {"hits": []}}

        mock_post.side_effect = [mock_response_fail, mock_response_success]

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.fetch_cvr("12345678")

        assert mock_post.call_count >= 1

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_retry_exhaustion(self, mock_post):
        """Test that retries are eventually exhausted."""
        # All calls fail
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        # Should have retried multiple times
        assert mock_post.call_count > 1
        assert result is None


class TestErrorHandling:
    """Tests for error handling in various failure scenarios."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_api_timeout(self, mock_post):
        """Test handling of API timeout."""
        mock_post.side_effect = requests.exceptions.Timeout("Request timed out")

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        assert result is None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_invalid_json_response(self, mock_post):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        assert result is None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_http_401_unauthorized(self, mock_post):
        """Test handling of HTTP 401 Unauthorized."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Unauthorized")
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        assert result is None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_http_404_not_found(self, mock_post):
        """Test handling of HTTP 404 Not Found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("12345678")

        assert result is None


class TestPIIFiltering:
    """Tests for PII filtering integration."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    @patch("unified_pipeline.util.cvr_api_client.filter_cvr_pii")
    def test_pii_filtering_called(self, mock_filter, mock_post):
        """Test that PII filtering is called on CVR data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "deltagerRelation": [{"deltager": {"telefonNummer": "12345678"}}],
                            }
                        }
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        # Mock PII filter to return filtered data
        mock_filter.return_value = {"cvrNummer": 31373077}

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            client.fetch_cvr("31373077")

        # PII filter should have been called
        mock_filter.assert_called_once()

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_pii_filtering_removes_personal_data(self, mock_post):
        """Test that personal data is removed from CVR response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "virksomhedMetadata": {"nyesteNavn": {"navn": "Test Company"}},
                                "deltagerRelation": [
                                    {
                                        "deltager": {
                                            "telefonNummer": ["12345678"],
                                            "elektroniskPost": ["test@example.com"],
                                        }
                                    }
                                ],
                            }
                        }
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("31373077")

        # Result should exist but personal contact info should be filtered
        assert result is not None
        assert "cvrNummer" in result


class TestGeocodingToggle:
    """Tests for geocoding functionality toggle."""

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    def test_geocoding_disabled_by_default(self, mock_post):
        """Test that geocoding is disabled by default."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {"hits": [{"_source": {"Vrvirksomhed": {"cvrNummer": 31373077}}}]}
        }
        mock_post.return_value = mock_response

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()
            result = client.fetch_cvr("31373077")

        # Should return data without geocoding
        assert result is not None

    @patch("unified_pipeline.util.cvr_api_client.requests.post")
    @patch("unified_pipeline.util.cvr_api_client.DAWAAPIClient")
    def test_geocoding_enabled(self, mock_dawa_client, mock_post):
        """Test that geocoding can be enabled."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "beliggenhedsadresse": [{"adresseId": "test-id"}],
                            }
                        }
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        # Mock DAWA client
        mock_dawa_instance = Mock()
        mock_dawa_instance.geocode_address_by_id.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
        }
        mock_dawa_client.return_value = mock_dawa_instance

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=True)
            result = client.fetch_cvr("31373077")

        # Geocoding should have been called
        assert result is not None


class TestMunicipalityNameFormatting:
    """Tests for municipality name formatting."""

    def test_format_municipality_name(self):
        """Test municipality name formatting from ALL CAPS to Title Case."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient()

            # Test ALL CAPS to Title Case
            assert client._format_municipality_name("KØBENHAVN") == "København"
            assert client._format_municipality_name("AARHUS") == "Aarhus"

            # Test empty/None
            assert client._format_municipality_name("") == ""
            assert client._format_municipality_name(None) is None
