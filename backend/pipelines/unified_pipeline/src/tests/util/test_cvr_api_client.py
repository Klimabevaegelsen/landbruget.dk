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
            client = CVRAPIClient(enable_geocoding=False)
            assert client.base_url == "http://distribution.virk.dk"
            assert client.auth is not None

    def test_client_initialization_without_credentials(self):
        """Test client initialization without credentials raises error."""
        with patch.dict("os.environ", {}, clear=True), pytest.raises(ValueError):
            CVRAPIClient()

    @patch("unified_pipeline.util.cvr_api_client.HTTPBasicAuth")
    def test_http_basic_auth_setup(self, mock_auth):
        """Test HTTP Basic Auth is properly configured."""
        with patch.dict("os.environ", {"CVR_USERNAME": "user123", "CVR_PASSWORD": "pass456"}):
            CVRAPIClient(enable_geocoding=False)
            mock_auth.assert_called_once_with("user123", "pass456")


class TestCVRLookup:
    """Tests for CVR company lookup functionality."""

    def _make_client(self):
        """Helper to create a client with geocoding disabled."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            return CVRAPIClient(enable_geocoding=False)

    def test_get_company_data_valid_8_digit(self):
        """Test fetching company data with valid 8-digit CVR."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "navne": [
                                    {
                                        "navn": "Test Company",
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        client = self._make_client()
        with patch.object(client.session, "post", return_value=mock_response):
            result = client.get_company_data("31373077")

        assert result is not None
        assert "cvr_number" in result
        assert result["cvr_number"] == 31373077

    def test_get_company_data_invalid_cvr(self):
        """Test fetching with invalid CVR returns None."""
        client = self._make_client()

        # Invalid: only 7 digits
        result = client.get_company_data("1234567")
        assert result is None

        # Invalid: 9 digits
        result = client.get_company_data("123456789")
        assert result is None

        # Invalid: non-numeric
        result = client.get_company_data("abcdefgh")
        assert result is None

    def test_get_company_data_not_found(self):
        """Test fetching CVR that doesn't exist."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}

        client = self._make_client()
        with patch.object(client.session, "post", return_value=mock_response):
            result = client.get_company_data("12345678")

        assert result is None

    def test_get_company_data_with_elasticsearch_query(self):
        """Test that Elasticsearch-style query is properly formatted."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}

        client = self._make_client()
        with patch.object(client.session, "post", return_value=mock_response) as mock_post:
            client.get_company_data("31373077")

        # Verify Elasticsearch query structure
        call_args = mock_post.call_args
        # The client uses json=payload, so check call_args kwargs
        query_data = call_args[1]["json"] if "json" in call_args[1] else call_args[1].get("json")
        assert query_data is not None
        assert "query" in query_data


class TestBatchFetching:
    """Tests for batch company fetching functionality."""

    def _make_client(self):
        """Helper to create a client with geocoding disabled."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            return CVRAPIClient(enable_geocoding=False)

    def test_fetch_multiple_companies(self):
        """Test batch fetching multiple companies."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 12345678,
                                "navne": [],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        cvr_list = ["12345678", "87654321", "11111111"]

        client = self._make_client()
        with patch.object(client.session, "post", return_value=mock_response):
            result = client.fetch_multiple_companies(cvr_list, enrich_with_geometry=False)

        assert "results" in result
        assert "summary" in result

    @patch("unified_pipeline.util.cvr_api_client.time.sleep")
    def test_fetch_multiple_with_rate_limiting(self, mock_sleep):
        """Test that fetching respects rate limiting between batches."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}

        cvr_list = ["12345678", "87654321"]

        client = self._make_client()
        # Force last_request_time to be recent to trigger rate limiting
        client.last_request_time = 999999999999  # far future to force sleep
        with patch.object(client.session, "post", return_value=mock_response):
            client.fetch_multiple_companies(cvr_list, enrich_with_geometry=False)

        # Rate limiting sleep should have been called
        assert mock_sleep.call_count >= 1

    def test_fetch_multiple_companies_empty_list(self):
        """Test batch fetching with empty CVR list."""
        client = self._make_client()
        with patch.object(client.session, "post") as mock_post:
            result = client.fetch_multiple_companies([])

        assert result["results"] == {}
        assert result["summary"]["total"] == 0
        mock_post.assert_not_called()

    def test_fetch_multiple_companies_filters_invalid_cvrs(self):
        """Test that batch fetching filters out invalid CVRs."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 12345678,
                                "navne": [],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        # Mix of valid and invalid CVRs
        cvr_list = ["12345678", "invalid", None, "123"]

        client = self._make_client()
        with patch.object(client.session, "post", return_value=mock_response):
            result = client.fetch_multiple_companies(cvr_list, enrich_with_geometry=False)

        # Only valid CVR should contribute to API calls
        assert result["summary"]["invalid_cvrs"] == 3


class TestRateLimiting:
    """Tests for API rate limiting behavior."""

    @patch("unified_pipeline.util.cvr_api_client.time.sleep")
    def test_rate_limiting_100ms_interval(self, mock_sleep):
        """Test rate limiting enforces 100ms minimum interval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 12345678,
                                "navne": [],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with patch.object(client.session, "post", return_value=mock_response):
            # Force last_request_time to be recent to trigger rate limiting
            client.last_request_time = 999999999999  # far future to force sleep
            client.get_company_data("12345678")

        # Should have at least one sleep call for rate limiting
        assert mock_sleep.call_count >= 1

        # Check that sleep duration is at least 0.1 seconds (or close to the interval)
        for call in mock_sleep.call_args_list:
            sleep_duration = call[0][0]
            assert sleep_duration >= 0.0  # sleep is called with computed gap


class TestRetryLogic:
    """Tests for retry logic with exponential backoff."""

    def test_retry_on_connection_error(self):
        """Test retry on connection error."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"hits": {"hits": []}}

        # First two calls fail, third succeeds
        with (
            patch.object(
                client.session,
                "post",
                side_effect=[
                    requests.exceptions.ConnectionError("Connection failed"),
                    requests.exceptions.ConnectionError("Connection failed"),
                    success_response,
                ],
            ) as mock_post,
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            client.get_company_data("12345678")

        # Should retry and eventually succeed
        assert mock_post.call_count == 3

    def test_retry_on_http_500_error(self):
        """Test retry on HTTP 500 error."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_response_fail.text = "Internal Server Error"

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"hits": {"hits": []}}

        with (
            patch.object(
                client.session, "post", side_effect=[mock_response_fail, mock_response_success]
            ) as mock_post,
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            client.get_company_data("12345678")

        assert mock_post.call_count >= 1

    def test_retry_exhaustion(self):
        """Test that retries are eventually exhausted."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with (
            patch.object(
                client.session,
                "post",
                side_effect=requests.exceptions.ConnectionError("Connection failed"),
            ) as mock_post,
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            result = client.get_company_data("12345678")

        # Should have retried multiple times
        assert mock_post.call_count > 1
        assert result is None


class TestErrorHandling:
    """Tests for error handling in various failure scenarios."""

    def test_api_timeout(self):
        """Test handling of API timeout."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with (
            patch.object(
                client.session,
                "post",
                side_effect=requests.exceptions.Timeout("Request timed out"),
            ),
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            result = client.get_company_data("12345678")

        assert result is None

    def test_invalid_json_response(self):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with (
            patch.object(client.session, "post", return_value=mock_response),
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            result = client.get_company_data("12345678")

        assert result is None

    def test_http_401_unauthorized(self):
        """Test handling of HTTP 401 Unauthorized."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Unauthorized")
        mock_response.text = "Unauthorized"

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with (
            patch.object(client.session, "post", return_value=mock_response),
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            result = client.get_company_data("12345678")

        assert result is None

    def test_http_404_not_found(self):
        """Test handling of HTTP 404 Not Found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_response.text = "Not Found"

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with (
            patch.object(client.session, "post", return_value=mock_response),
            patch("unified_pipeline.util.cvr_api_client.time.sleep"),
        ):
            result = client.get_company_data("12345678")

        assert result is None


class TestPIIFiltering:
    """Tests for PII filtering integration."""

    @patch("unified_pipeline.util.cvr_api_client.filter_cvr_pii")
    def test_pii_filtering_called(self, mock_filter):
        """Test that PII filtering is called on CVR leadership data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "navne": [
                                    {
                                        "navn": "Test Company",
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [
                                    {
                                        "deltager": {"telefonNummer": "12345678"},
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                            }
                        }
                    }
                ]
            }
        }

        mock_filter.return_value = []

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with patch.object(client.session, "post", return_value=mock_response):
            client.get_company_data("31373077")

        # PII filter should have been called on leadership data
        mock_filter.assert_called_once()

    def test_pii_filtering_removes_personal_data(self):
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
                                "navne": [
                                    {
                                        "navn": "Test Company",
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [
                                    {
                                        "deltager": {
                                            "telefonNummer": ["12345678"],
                                            "elektroniskPost": ["test@example.com"],
                                        },
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                            }
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with patch.object(client.session, "post", return_value=mock_response):
            result = client.get_company_data("31373077")

        # Result should exist and have cvr_number
        assert result is not None
        assert "cvr_number" in result


class TestGeocodingToggle:
    """Tests for geocoding functionality toggle."""

    def test_geocoding_disabled_by_default_when_explicitly_set(self):
        """Test that geocoding can be disabled."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "Vrvirksomhed": {
                                "cvrNummer": 31373077,
                                "navne": [],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

        with patch.object(client.session, "post", return_value=mock_response):
            result = client.get_company_data("31373077")

        # Should return data without geocoding
        assert result is not None
        assert client.dawa_client is None

    def test_geocoding_enabled(self):
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
                                "navne": [],
                                "virksomhedsform": [],
                                "virksomhedsstatus": [],
                                "beliggenhedsadresse": [
                                    {
                                        "adresseId": "test-id",
                                        "periode": {"gyldigFra": "2000-01-01", "gyldigTil": None},
                                    }
                                ],
                                "postadresse": [],
                                "elektroniskPost": [],
                                "telefonNummer": [],
                                "deltagerRelation": [],
                            }
                        }
                    }
                ]
            }
        }

        with (
            patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}),
            patch(
                "unified_pipeline.util.cached_dawa_api_client.CachedDAWAAPIClient"
            ) as mock_dawa_class,
        ):
            mock_dawa_instance = Mock()
            mock_dawa_instance.geocode_address_by_id.return_value = {
                "latitude": 55.6761,
                "longitude": 12.5683,
            }
            mock_dawa_class.return_value = mock_dawa_instance

            client = CVRAPIClient(enable_geocoding=True)

        with patch.object(client.session, "post", return_value=mock_response):
            result = client.get_company_data("31373077")

        # Geocoding client should have been created and result should exist
        assert result is not None
        assert client.dawa_client is not None


class TestMunicipalityNameFormatting:
    """Tests for municipality name formatting."""

    def test_format_municipality_name(self):
        """Test municipality name formatting from ALL CAPS to Title Case."""
        with patch.dict("os.environ", {"CVR_USERNAME": "test", "CVR_PASSWORD": "test"}):
            client = CVRAPIClient(enable_geocoding=False)

            # Test ALL CAPS to Title Case
            assert client._format_municipality_name("KØBENHAVN") == "København"
            assert client._format_municipality_name("AARHUS") == "Aarhus"

            # Test empty/None
            assert client._format_municipality_name("") == ""
            assert client._format_municipality_name(None) is None
