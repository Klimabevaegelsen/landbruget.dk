"""Tests for the Adressevælger geocoding client."""

from unittest.mock import Mock, patch

import pytest
import requests

from unified_pipeline.util.adressevaelger_client import AdressevaelgerClient


def make_response(data, status_code=200, headers=None):
    response = Mock()
    response.status_code = status_code
    response.headers = headers or {}
    response.json.return_value = data
    if status_code >= 400 and status_code != 404:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    return response


ADRESSE_DETAIL = {
    "status": "ok",
    "adresse": {
        "id_lokalid": "address-id",
        "adressebetegnelse": "Rådhuspladsen 1, 2. th, 1550 København V",
        "etagebetegnelse": "2.",
        "doerbetegnelse": "th",
        "husnummer": {
            "id_lokalid": "house-id",
            "husnummertekst": "1",
            "adgangsadressebetegnelse": "Rådhuspladsen 1, 1550 København V",
            "vejnavn": "Rådhuspladsen",
            "adgangspunkt": {"koordinater": {"x": 724434.93, "y": 6175755.61}},
            "postnummer": {"postnr": "1550", "navn": "København V"},
            "navngivenvejkommunedel": {"kommune": "0101"},
        },
    },
}

HUSNUMMER_DETAIL = {
    "status": "ok",
    "husnummer": {
        "id_lokalid": "house-id",
        "husnummertekst": "1",
        "adgangsadressebetegnelse": "Rådhuspladsen 1, 1550 København V",
        "vejnavn": "Rådhuspladsen",
        "adgangspunkt": {
            "geometri": {"coordinates": [724434.93, 6175755.61]},
        },
        "postnummer": {"postnr": "1550", "navn": "København V"},
        "navngivenvejkommunedel": {"kommune": "0101"},
    },
}

SEARCH_RESULTS = {
    "status": "ok",
    "beskrivelse": "",
    "fund": [
        {"type": "adresse", "id": "address-id", "titel": "Nørregade 1, 6000 Kolding"},
        {"type": "navngivenvejpostnummer", "id": "street-id", "titel": "Nørregade, 6000 Kolding"},
        {"type": "husnummer", "id": "house-id", "titel": "Nørregade 1, 6000 Kolding"},
    ],
}


class TestAdressevaelgerClient:
    def test_client_configuration(self):
        client = AdressevaelgerClient()

        assert client.base_url == "https://adressevaelger.dk"
        assert client.token == "adressevaelger123"
        assert client.session.headers["User-Agent"] == "landbrugsdata-cvr-enrichment/1.0"

    def test_empty_env_vars_fall_back_to_public_defaults(self, monkeypatch):
        # GitHub Actions sets unset secrets to "", which must not become an empty token.
        monkeypatch.setenv("ADRESSEVAELGER_API_URL", "")
        monkeypatch.setenv("ADRESSEVAELGER_TOKEN", "")

        client = AdressevaelgerClient()

        assert client.base_url == "https://adressevaelger.dk"
        assert client.token == "adressevaelger123"

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_id_lookup_parses_adresse_with_floor_and_door(self, mock_get):
        mock_get.return_value = make_response(ADRESSE_DETAIL)

        result = AdressevaelgerClient().geocode_address_by_id("address-id")

        assert result is not None
        assert set(result) == {
            "adresse_id",
            "latitude",
            "longitude",
            "coordinate_system",
            "srid",
            "full_address",
            "street_name",
            "house_number",
            "floor",
            "door",
            "postal_code",
            "city",
            "municipality_code",
            "municipality_name",
            "coordinate_quality",
            "coordinate_source",
            "dawa_fetch_timestamp",
        }
        assert result["adresse_id"] == "address-id"
        assert result["full_address"] == "Rådhuspladsen 1, 2. th, 1550 København V"
        assert result["street_name"] == "Rådhuspladsen"
        assert result["house_number"] == "1"
        assert result["floor"] == "2."
        assert result["door"] == "th"
        assert result["postal_code"] == "1550"
        assert result["city"] == "København V"
        assert result["municipality_code"] == "0101"
        assert result["municipality_name"] is None
        assert result["coordinate_quality"] is None
        assert result["coordinate_source"] == "adressevaelger"
        assert result["coordinate_system"] == "WGS84"
        assert result["srid"] == 4326
        assert result["latitude"] == pytest.approx(55.6757, abs=0.0001)
        assert result["longitude"] == pytest.approx(12.5690, abs=0.001)

        mock_get.assert_called_once_with(
            "https://adressevaelger.dk/adresser/address-id",
            params={"token": "adressevaelger123"},
            timeout=30,
        )

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_id_lookup_falls_back_to_husnummer_after_404(self, mock_get):
        mock_get.side_effect = [make_response(None, 404), make_response(HUSNUMMER_DETAIL)]

        result = AdressevaelgerClient().geocode_address_by_id("house-id")

        assert result is not None
        assert result["street_name"] == "Rådhuspladsen"
        assert result["house_number"] == "1"
        assert result["latitude"] == pytest.approx(55.6757, abs=0.0001)
        assert [call.args[0] for call in mock_get.call_args_list] == [
            "https://adressevaelger.dk/adresser/house-id",
            "https://adressevaelger.dk/husnumre/house-id",
        ]

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_empty_id_and_missing_coordinates_return_none(self, mock_get):
        client = AdressevaelgerClient()
        assert client.geocode_address_by_id("") is None
        mock_get.assert_not_called()

        missing_coordinates = {"status": "ok", "adresse": {"husnummer": {}}}
        mock_get.return_value = make_response(missing_coordinates)
        assert client.geocode_address_by_id("missing-coordinates") is None

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_non_finite_coordinates_return_none(self, mock_get):
        response = {
            "status": "ok",
            "husnummer": {"adgangspunkt": {"koordinater": {"x": float("inf"), "y": 1}}},
        }
        mock_get.return_value = make_response(response)

        assert AdressevaelgerClient().geocode_address_by_id("invalid") is None

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_search_returns_only_selectable_fund_items(self, mock_get):
        mock_get.return_value = make_response(SEARCH_RESULTS)
        client = AdressevaelgerClient()

        results = client.search_address("Nørregade 1 6000", limit=5)

        assert [item["type"] for item in results] == ["adresse", "husnummer"]
        mock_get.assert_called_once_with(
            "https://adressevaelger.dk/adresser/soeg",
            params={"tekst": "Nørregade 1 6000", "maksimum": 5, "token": "adressevaelger123"},
            timeout=30,
        )

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_search_rejects_empty_short_and_too_long_queries_without_request(self, mock_get):
        client = AdressevaelgerClient()

        assert client.search_address("") == []
        assert client.search_address("a") == []
        assert client.search_address("a" * 74) == []
        mock_get.assert_not_called()

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_status_fejl_returns_no_search_results(self, mock_get):
        mock_get.return_value = make_response(
            {"status": "fejl", "beskrivelse": "Invalid query", "fund": []}
        )

        assert AdressevaelgerClient().search_address("Nørregade") == []

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_free_text_accepts_matching_postcode_token(self, mock_get):
        mock_get.side_effect = [
            make_response(SEARCH_RESULTS),
            make_response(ADRESSE_DETAIL),
        ]

        result = AdressevaelgerClient().geocode_free_text("Nørregade 1", "6000", "Kolding")

        assert result is not None
        assert result["datavask_enriched"] is True
        assert result["dawa_enriched"] is True
        assert mock_get.call_args_list[0].kwargs["params"]["tekst"] == "Nørregade 1 6000"
        assert mock_get.call_args_list[1].args[0] == "https://adressevaelger.dk/adresser/address-id"

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_free_text_rejects_postcode_prefix_match(self, mock_get):
        mismatched = {
            "status": "ok",
            "beskrivelse": "",
            "fund": [
                {"type": "adresse", "id": "address-id", "titel": "Nørregade 1, 60001 Kolding"}
            ],
        }
        mock_get.side_effect = [make_response(mismatched)] * 3

        result = AdressevaelgerClient().geocode_free_text("Nørregade 1", "6000", "Kolding")

        assert result is None
        assert [call.kwargs["params"]["tekst"] for call in mock_get.call_args_list] == [
            "Nørregade 1 6000",
            "Nørregade 1, 6000 Kolding",
            "Nørregade 1",
        ]

    def test_free_text_rejects_other_house_number_or_street(self):
        client = AdressevaelgerClient()
        results = [
            {"type": "adresse", "id": "ten", "titel": "Nørregade 10, 6000 Kolding"},
            {"type": "adresse", "id": "vej", "titel": "Nørregadevej 1, 6000 Kolding"},
        ]
        with (
            patch.object(client, "search_address", return_value=results),
            patch.object(client, "geocode_address_by_id") as by_id,
        ):
            assert client.geocode_free_text("Nørregade 1", "6000", "Kolding") is None
        by_id.assert_not_called()

    def test_free_text_accepts_floor_suffix_and_house_letter_spacing(self):
        client = AdressevaelgerClient()
        results = [
            {"type": "adresse", "id": "match", "titel": "Vestergade 2A, 1., 1456 København K"}
        ]
        with (
            patch.object(client, "search_address", return_value=results),
            patch.object(client, "geocode_address_by_id", return_value={"latitude": 1.0}) as by_id,
        ):
            assert client.geocode_free_text("Vestergade 2 A 1.", "1456") is not None
        by_id.assert_called_once_with("match", result_type="adresse")

    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_free_text_query_order_and_long_query_skipping(self, mock_get):
        client = AdressevaelgerClient()
        with patch.object(client, "search_address", return_value=[]) as search:
            assert client.geocode_free_text("A" * 70, "1234", "X") is None

        search.assert_called_once_with("A" * 70, limit=5)
        mock_get.assert_not_called()

        with patch.object(client, "search_address", side_effect=[[], [], []]) as search:
            assert client.geocode_free_text("Nørregade 1", "6000", "Kolding") is None

        assert [call.args[0] for call in search.call_args_list] == [
            "Nørregade 1 6000",
            "Nørregade 1, 6000 Kolding",
            "Nørregade 1",
        ]

    @patch("unified_pipeline.util.adressevaelger_client.time.sleep")
    @patch("unified_pipeline.util.adressevaelger_client.requests.Session.get")
    def test_429_retries_after_retry_after_header(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            make_response({}, 429, {"Retry-After": "7"}),
            make_response(ADRESSE_DETAIL),
        ]

        result = AdressevaelgerClient().geocode_address_by_id("address-id")

        assert result is not None
        mock_sleep.assert_called_once_with(7)
        assert mock_get.call_count == 2

    def test_retry_exhaustion_returns_none(self, monkeypatch):
        response = make_response({}, 503)
        monkeypatch.setattr(AdressevaelgerClient._make_request.retry, "sleep", lambda _delay: None)
        with patch(
            "unified_pipeline.util.adressevaelger_client.requests.Session.get",
            return_value=response,
        ) as mock_get:
            assert AdressevaelgerClient().geocode_address_by_id("unavailable") is None

        assert mock_get.call_count == 3

    def test_geometry_helpers(self):
        client = AdressevaelgerClient()

        assert client.create_geometry_wkt(55.6761, 12.5683) == "POINT(12.5683 55.6761)"
        assert client.create_geometry_geojson(55.6761, 12.5683) == {
            "type": "Point",
            "coordinates": [12.5683, 55.6761],
        }
