"""
Tests for DAWA API Client.

Tests for: unified_pipeline/util/dawa_api_client.py

Covers:
- Address geocoding by DAWA ID
- Address validation
- Reverse geocoding
- Danish address format handling
- Coordinate validation (WGS84)
- API error handling
- Rate limiting
- Retry logic
"""

import json
import time
from unittest.mock import Mock, patch

import pytest
import requests

from unified_pipeline.util.dawa_api_client import DAWAAPIClient


class TestDAWAAPIClient:
    """Tests for DAWA API client initialization."""

    def test_client_initialization(self):
        """Test DAWA API client can be initialized."""
        client = DAWAAPIClient()
        assert client.base_url == "https://api.dataforsyningen.dk"
        assert client.session is not None

    def test_user_agent_header_set(self):
        """Test that User-Agent header is set."""
        client = DAWAAPIClient()
        assert "User-Agent" in client.session.headers
        assert "landbrugsdata" in client.session.headers["User-Agent"].lower()


class TestGeocodeAddressByID:
    """Tests for geocoding addresses by DAWA address ID."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_valid_address_id(self, mock_get):
        """Test geocoding with valid DAWA address ID."""
        # Mock successful DAWA response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "0a3f50c8-5f7d-32b8-e044-0003ba298018",
            "adressebetegnelse": "Rødkildevej 46, 2400 København NV",
            "adgangsadresse": {
                "adgangspunkt": {
                    "koordinater": [12.5683, 55.6761],  # [lon, lat] in WGS84
                    "nøjagtighed": "A",
                    "kilde": 1,
                },
                "vejstykke": {"navn": "Rødkildevej"},
                "husnr": "46",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("0a3f50c8-5f7d-32b8-e044-0003ba298018")

        assert result is not None
        assert result["latitude"] == 55.6761
        assert result["longitude"] == 12.5683
        assert result["coordinate_system"] == "WGS84"
        assert result["srid"] == 4326
        assert result["full_address"] == "Rødkildevej 46, 2400 København NV"

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_with_floor_and_door(self, mock_get):
        """Test geocoding address with floor and door information."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adressebetegnelse": "Rødkildevej 46, 2. th, 2400 København NV",
            "etage": "2",
            "dør": "th",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5683, 55.6761]},
                "vejstykke": {"navn": "Rødkildevej"},
                "husnr": "46",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result is not None
        assert result["floor"] == "2"
        assert result["door"] == "th"

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_empty_address_id(self, mock_get):
        """Test geocoding with empty address ID."""
        client = DAWAAPIClient()

        result = client.geocode_address_by_id("")
        assert result is None

        result = client.geocode_address_by_id(None)
        assert result is None

        mock_get.assert_not_called()

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_address_not_found(self, mock_get):
        """Test geocoding with non-existent address ID."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = None
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("non-existent-id")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_invalid_coordinates(self, mock_get):
        """Test geocoding with invalid coordinates in response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": []},  # Invalid: empty coordinates
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_wgs84_coordinate_system(self, mock_get):
        """Test that coordinates are returned in WGS84 (EPSG:4326)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5683, 55.6761]},
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result["coordinate_system"] == "WGS84"
        assert result["srid"] == 4326
        # DAWA returns [longitude, latitude] format
        assert result["longitude"] == 12.5683
        assert result["latitude"] == 55.6761


class TestSearchAddress:
    """Tests for text-based address search."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_search_address_basic(self, mock_get):
        """Test basic address search."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "test-id",
                "adressebetegnelse": "Rødkildevej 46, 2400 København NV",
                "adgangsadresse": {
                    "adgangspunkt": {"koordinater": [12.5683, 55.6761]},
                    "vejstykke": {"navn": "Rødkildevej"},
                    "husnr": "46",
                    "postnummer": {"nr": "2400", "navn": "København NV"},
                    "kommune": {"kode": "0101", "navn": "København"},
                },
            }
        ]
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        results = client.search_address("Rødkildevej 46")

        assert len(results) == 1
        assert results[0]["full_address"] == "Rødkildevej 46, 2400 København NV"
        assert results[0]["latitude"] == 55.6761
        assert results[0]["longitude"] == 12.5683

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_search_address_limit(self, mock_get):
        """Test address search with result limit."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "id1", "adgangsadresse": {"adgangspunkt": {"koordinater": [12.5, 55.6]}}},
            {"id": "id2", "adgangsadresse": {"adgangspunkt": {"koordinater": [12.6, 55.7]}}},
        ]
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        results = client.search_address("Testvej", limit=5)

        # Verify limit parameter is passed
        call_args = mock_get.call_args
        assert call_args[1]["params"]["per_side"] == 5

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_search_address_no_results(self, mock_get):
        """Test address search with no results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        results = client.search_address("NonExistentStreet 999")

        assert results == []

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_search_address_skip_invalid_coordinates(self, mock_get):
        """Test that addresses with invalid coordinates are skipped."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "id1",
                "adgangsadresse": {"adgangspunkt": {"koordinater": []}},  # Invalid
            },
            {
                "id": "id2",
                "adgangsadresse": {"adgangspunkt": {"koordinater": [12.5, 55.6]}},  # Valid
            },
        ]
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        results = client.search_address("Testvej")

        # Only valid coordinates should be returned
        assert len(results) == 1
        assert results[0]["latitude"] == 55.6


class TestBatchGeocoding:
    """Tests for batch address geocoding."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    @patch("unified_pipeline.util.dawa_api_client.time.sleep")
    def test_batch_geocode_addresses(self, mock_sleep, mock_get):
        """Test batch geocoding of multiple addresses."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5, 55.6]},
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        address_ids = ["id1", "id2", "id3"]

        client = DAWAAPIClient()
        results = client.geocode_addresses_batch(address_ids)

        assert len(results) == 3
        assert mock_get.call_count == 3
        # Should have delays between requests
        assert mock_sleep.call_count >= 2

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_batch_geocode_empty_list(self, mock_get):
        """Test batch geocoding with empty list."""
        client = DAWAAPIClient()
        results = client.geocode_addresses_batch([])

        assert results == {}
        mock_get.assert_not_called()

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_batch_geocode_filters_none_values(self, mock_get):
        """Test that None values are filtered in batch geocoding."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5, 55.6]},
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }
        mock_get.return_value = mock_response

        address_ids = ["id1", None, "", "id2"]

        client = DAWAAPIClient()
        results = client.geocode_addresses_batch(address_ids)

        # Should only process non-empty IDs
        assert mock_get.call_count == 2


class TestDatavaskGeocoding:
    """Tests for Datavask API geocoding fallback."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_with_datavask_success(self, mock_get):
        """Test successful geocoding with Datavask API."""

        def side_effect(*args, **kwargs):
            if "datavask" in args[0]:
                # Datavask response
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    "resultater": [
                        {
                            "adresse": {
                                "id": "test-id",
                                "etage": "2",
                                "dør": "th",
                            }
                        }
                    ]
                }
                return response
            else:
                # Regular DAWA response
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    "id": "test-id",
                    "adgangsadresse": {
                        "adgangspunkt": {"koordinater": [12.5, 55.6]},
                        "vejstykke": {"navn": "Testvej"},
                        "husnr": "1",
                        "postnummer": {"nr": "2400", "navn": "København NV"},
                        "kommune": {"kode": "0101", "navn": "København"},
                    },
                }
                return response

        mock_get.side_effect = side_effect

        client = DAWAAPIClient()
        result = client.geocode_with_datavask("Testvej 1, 2. th, 2400 København NV")

        assert result is not None
        assert result["latitude"] == 55.6
        assert result["datavask_enriched"] is True
        assert result["dawa_enriched"] is True

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_with_datavask_no_results(self, mock_get):
        """Test Datavask with no results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"resultater": []}
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_with_datavask("Invalid Address XYZ")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_geocode_with_datavask_empty_address(self, mock_get):
        """Test Datavask with empty address."""
        # Mock empty results response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"resultater": []}
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_with_datavask("")

        # Empty address still calls API but returns None due to no results
        assert result is None


class TestErrorHandling:
    """Tests for API error handling."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_handle_timeout(self, mock_get):
        """Test handling of request timeout."""
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_handle_connection_error(self, mock_get):
        """Test handling of connection error."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_handle_http_error(self, mock_get):
        """Test handling of HTTP error."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server error")
        mock_get.return_value = mock_response

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        assert result is None

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_handle_rate_limit_429(self, mock_get):
        """Test handling of rate limit (HTTP 429)."""
        # First request returns 429, second succeeds
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "1"}

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5, 55.6]},
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }

        mock_get.side_effect = [mock_response_429, mock_response_success]

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should retry and succeed
        assert result is not None
        assert mock_get.call_count == 2


class TestGeometryHelpers:
    """Tests for geometry helper functions."""

    def test_create_geometry_wkt(self):
        """Test WKT geometry creation."""
        client = DAWAAPIClient()
        wkt = client.create_geometry_wkt(55.6761, 12.5683)

        assert wkt == "POINT(12.5683 55.6761)"

    def test_create_geometry_geojson(self):
        """Test GeoJSON geometry creation."""
        client = DAWAAPIClient()
        geojson = client.create_geometry_geojson(55.6761, 12.5683)

        assert geojson["type"] == "Point"
        assert geojson["coordinates"] == [12.5683, 55.6761]  # [lon, lat]


class TestRetryLogic:
    """Tests for retry logic with exponential backoff."""

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_retry_on_connection_error(self, mock_get):
        """Test retry on connection error."""
        # First two calls fail, third succeeds
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "test-id",
            "adgangsadresse": {
                "adgangspunkt": {"koordinater": [12.5, 55.6]},
                "vejstykke": {"navn": "Testvej"},
                "husnr": "1",
                "postnummer": {"nr": "2400", "navn": "København NV"},
                "kommune": {"kode": "0101", "navn": "København"},
            },
        }

        mock_get.side_effect = [
            requests.exceptions.RequestException("Error 1"),
            requests.exceptions.RequestException("Error 2"),
            mock_response,
        ]

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should retry and eventually succeed
        assert result is not None
        assert mock_get.call_count == 3

    @patch("unified_pipeline.util.dawa_api_client.requests.Session.get")
    def test_retry_exhaustion(self, mock_get):
        """Test that retries are exhausted after max attempts."""
        mock_get.side_effect = requests.exceptions.RequestException("Persistent error")

        client = DAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should have retried multiple times
        assert mock_get.call_count > 1
        assert result is None
