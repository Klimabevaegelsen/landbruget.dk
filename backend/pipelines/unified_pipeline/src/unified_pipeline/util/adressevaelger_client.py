"""Adressevælger client for CVR address geocoding."""

import math
import os
import re
import time
from typing import Any, ClassVar

import requests
from common.crs_utils import utm32_to_wgs84
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.util.log_util import Logger


class AdressevaelgerClient:
    """Search and geocode Danish addresses through Adressevælger."""

    SELECTABLE_TYPES: ClassVar[frozenset[str]] = frozenset({"adresse", "husnummer"})

    def __init__(self):
        self.log = Logger.get_logger()
        self.base_url = (os.getenv("ADRESSEVAELGER_API_URL") or "https://adressevaelger.dk").rstrip(
            "/"
        )
        self.token = os.getenv("ADRESSEVAELGER_TOKEN") or "adressevaelger123"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "landbrugsdata-cvr-enrichment/1.0"})

    @retry(
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        stop=stop_after_attempt(3),
    )
    def _make_request(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | list[Any] | None:
        """Make a request with the legacy retry, timeout, and rate-limit behavior."""
        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 404:
                return None

            if response.status_code == 429:
                try:
                    retry_after = int(response.headers.get("Retry-After", 5))
                except (TypeError, ValueError):
                    retry_after = 5
                self.log.warning("Adressevælger rate limit hit, waiting {} seconds", retry_after)
                time.sleep(retry_after)
                response = self.session.get(url, params=params, timeout=30)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            self.log.error("Adressevælger API request error: {}", error)
            raise

    @staticmethod
    def _finite_number(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @classmethod
    def _coordinates(cls, detail: dict[str, Any]) -> tuple[float, float] | None:
        address = detail.get("adresse") or detail.get("husnummer")
        if not isinstance(address, dict):
            return None

        house_number = address.get("husnummer")
        if not isinstance(house_number, dict):
            house_number = address
        access_point = house_number.get("adgangspunkt")
        if not isinstance(access_point, dict):
            return None

        coordinates = access_point.get("koordinater")
        if isinstance(coordinates, dict):
            x = cls._finite_number(coordinates.get("x"))
            y = cls._finite_number(coordinates.get("y"))
            if x is not None and y is not None:
                return x, y

        geometry = access_point.get("geometri")
        pair = geometry.get("coordinates") if isinstance(geometry, dict) else None
        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
            x = cls._finite_number(pair[0])
            y = cls._finite_number(pair[1])
            if x is not None and y is not None:
                return x, y
        return None

    def geocode_address_by_id(
        self, address_id: str, *, result_type: str | None = None
    ) -> dict[str, Any] | None:
        """Resolve a DAR UUID, trying the address endpoint before house number."""
        if not address_id:
            return None

        resources = ["husnumre"] if result_type == "husnummer" else ["adresser", "husnumre"]
        try:
            data = None
            for resource in resources:
                data = self._make_request(
                    f"{self.base_url}/{resource}/{address_id}", {"token": self.token}
                )
                if data is not None:
                    break

            if not isinstance(data, dict) or data.get("status") == "fejl":
                return None

            projected = self._coordinates(data)
            if projected is None:
                self.log.warning("Missing or invalid Adressevælger coordinates for {}", address_id)
                return None

            x, y = projected
            longitude, latitude = utm32_to_wgs84(x, y)
            if not math.isfinite(latitude) or not math.isfinite(longitude):
                return None

            address = data.get("adresse") or data.get("husnummer") or {}
            house_number = address.get("husnummer")
            if not isinstance(house_number, dict):
                house_number = address
            postal_code = house_number.get("postnummer") or {}
            road = house_number.get("navngivenvejkommunedel") or {}
            full_address = address.get("adressebetegnelse") or address.get(
                "adgangsadressebetegnelse"
            )

            return {
                "adresse_id": address_id,
                "latitude": latitude,
                "longitude": longitude,
                "coordinate_system": "WGS84",
                "srid": 4326,
                "full_address": full_address,
                "street_name": house_number.get("vejnavn"),
                "house_number": house_number.get("husnummertekst"),
                "floor": address.get("etagebetegnelse"),
                "door": address.get("doerbetegnelse"),
                "postal_code": postal_code.get("postnr"),
                "city": postal_code.get("navn"),
                "municipality_code": road.get("kommune"),
                "municipality_name": None,
                "coordinate_quality": None,
                "coordinate_source": "adressevaelger",
                "dawa_fetch_timestamp": time.time(),
            }
        except Exception as error:
            self.log.error("Error geocoding address ID {}: {}", address_id, error)
            return None

    def search_address(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        """Return raw selectable Adressevælger search results."""
        if not query or len(query) < 2 or len(query) > 73:
            return []

        try:
            data = self._make_request(
                f"{self.base_url}/adresser/soeg",
                {"tekst": query, "maksimum": limit, "token": self.token},
            )
            if not isinstance(data, dict) or data.get("status") == "fejl":
                if isinstance(data, dict) and data.get("status") == "fejl":
                    self.log.warning("Adressevælger search failed: {}", data.get("beskrivelse", ""))
                return []

            funds = data.get("fund")
            if not isinstance(funds, list):
                return []
            return [
                result
                for result in funds
                if isinstance(result, dict) and result.get("type") in self.SELECTABLE_TYPES
            ]
        except Exception as error:
            self.log.error("Error searching Adressevælger for {!r}: {}", query, error)
            return []

    def geocode_free_text(
        self, street_address: str, postal_code: str | None = None, city: str | None = None
    ) -> dict[str, Any] | None:
        """Geocode strict address queries, requiring a matching postcode when provided."""
        if not street_address:
            return None

        queries = []
        if postal_code:
            queries.append(f"{street_address} {postal_code}")
        if postal_code and city:
            queries.append(f"{street_address}, {postal_code} {city}")
        queries.append(street_address)

        expected_street = self._street_and_number(street_address)
        postcode_pattern = (
            re.compile(rf"\b{re.escape(str(postal_code))}\b") if postal_code else None
        )

        def matches(result: dict[str, Any]) -> bool:
            title = str(result.get("titel", ""))
            if expected_street and self._street_and_number(title) != expected_street:
                return False
            return postcode_pattern is None or bool(postcode_pattern.search(title))

        for query in queries:
            if len(query) > 73:
                continue
            results = [result for result in self.search_address(query, limit=5) if matches(result)]
            if postal_code:
                hit = results[0] if results else None
            else:
                hit = results[0] if len(results) == 1 else None

            if not hit or not hit.get("id"):
                continue

            geocoded = self.geocode_address_by_id(hit["id"], result_type=hit.get("type"))
            if geocoded:
                geocoded["datavask_enriched"] = True
                geocoded["dawa_enriched"] = True
                return geocoded

        return None

    @staticmethod
    def _street_and_number(text: str) -> str | None:
        """Normalise the leading "street housenumber[letter]" part, e.g. "nørregade 2a"."""
        match = re.match(r"\s*(.*?\D)\s*(\d+)\s*([A-Za-zÆØÅæøå])?(?=[\s,.]|$)", text)
        if not match:
            return None
        street = " ".join(match.group(1).replace(",", " ").split()).lower()
        letter = (match.group(3) or "").lower()
        return f"{street} {match.group(2)}{letter}"

    @staticmethod
    def create_geometry_wkt(latitude: float, longitude: float) -> str:
        """Create a WKT POINT from WGS84 coordinates."""
        return f"POINT({longitude} {latitude})"

    @staticmethod
    def create_geometry_geojson(latitude: float, longitude: float) -> dict[str, Any]:
        """Create a GeoJSON Point from WGS84 coordinates."""
        return {"type": "Point", "coordinates": [longitude, latitude]}
