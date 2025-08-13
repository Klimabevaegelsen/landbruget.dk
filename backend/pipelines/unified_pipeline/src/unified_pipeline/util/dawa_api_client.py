"""
DAWA API Client
===============

Client for accessing the Danish Address Web API (DAWA) to geocode addresses.
Used to convert CVR address IDs to geographic coordinates.

DAWA API documentation: https://api.dataforsyningen.dk/
"""

import time
from typing import Any, Dict, List, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.util.log_util import Logger


class DAWAAPIClient:
    """
    Client for accessing the Danish Address Web API (DAWA).

    Provides methods to:
    - Geocode addresses using adresseId from CVR
    - Search for addresses by text
    - Handle rate limiting and errors gracefully
    """

    def __init__(self):
        self.log = Logger.get_logger()
        self.base_url = "https://api.dataforsyningen.dk"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "landbrugsdata-cvr-enrichment/1.0"})

    @retry(
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        stop=stop_after_attempt(3),
    )
    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to DAWA API with retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            API response data

        Raises:
            requests.exceptions.RequestException: For request failures
        """
        try:
            response = self.session.get(url, params=params, timeout=30)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                self.log.warning(f"DAWA API rate limit hit, waiting {retry_after}s")
                time.sleep(retry_after)
                response = self.session.get(url, params=params, timeout=30)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            self.log.error(f"DAWA API request error: {e}")
            raise

    def geocode_address_by_id(self, adresse_id: str) -> Optional[Dict[str, Any]]:
        """
        Geocode an address using its DAWA address ID.

        Args:
            adresse_id: UUID address identifier from CVR

        Returns:
            Dictionary with address and coordinate information, or None if not found
        """
        if not adresse_id:
            return None

        try:
            url = f"{self.base_url}/adresser/{adresse_id}"
            data = self._make_request(url)

            if not data:
                self.log.debug(f"No address data found for ID: {adresse_id}")
                return None

            # Extract coordinates from the nested adgangsadresse structure
            coordinates = (
                data.get("adgangsadresse", {}).get("adgangspunkt", {}).get("koordinater", [])
            )

            if len(coordinates) != 2:
                self.log.warning(f"Invalid coordinates for address ID {adresse_id}: {coordinates}")
                return None

            # DAWA returns coordinates as [longitude, latitude] in WGS84 (SRID 4326) by default
            # This is exactly what we want - WGS84 coordinates
            longitude, latitude = coordinates

            # Extract data from the nested structure
            adgangsadresse = data.get("adgangsadresse", {})

            return {
                "adresse_id": adresse_id,
                "latitude": latitude,  # WGS84 latitude
                "longitude": longitude,  # WGS84 longitude
                "coordinate_system": "WGS84",  # Explicit coordinate system
                "srid": 4326,  # EPSG code for WGS84
                "full_address": data.get("adressebetegnelse"),
                "street_name": adgangsadresse.get("vejstykke", {}).get("navn"),
                "house_number": adgangsadresse.get("husnr"),
                "floor": data.get("etage"),
                "door": data.get("dør"),
                "postal_code": adgangsadresse.get("postnummer", {}).get("nr"),
                "city": adgangsadresse.get("postnummer", {}).get("navn"),
                "municipality_code": adgangsadresse.get("kommune", {}).get("kode"),
                "municipality_name": adgangsadresse.get("kommune", {}).get("navn"),
                "coordinate_quality": adgangsadresse.get("adgangspunkt", {}).get("nøjagtighed"),
                "coordinate_source": adgangsadresse.get("adgangspunkt", {}).get("kilde"),
                "dawa_fetch_timestamp": time.time(),
            }

        except Exception as e:
            self.log.error(f"Error geocoding address ID {adresse_id}: {e}")
            return None

    def geocode_addresses_batch(
        self, address_ids: List[str], max_concurrent: int = 5
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Geocode multiple addresses by ID.

        Args:
            address_ids: List of DAWA address IDs
            max_concurrent: Maximum number of concurrent requests 
                (not implemented yet - sequential for now)

        Returns:
            Dictionary mapping address_id to geocoded data (or None if failed)
        """
        results = {}

        self.log.info(f"Geocoding {len(address_ids)} addresses via DAWA API")

        for address_id in address_ids:
            if address_id:  # Skip None/empty IDs
                results[address_id] = self.geocode_address_by_id(address_id)
                time.sleep(0.1)  # Small delay to be respectful to the API
            else:
                results[address_id] = None

        successful = len([r for r in results.values() if r is not None])
        self.log.info(f"Successfully geocoded {successful}/{len(address_ids)} addresses")

        return results

    def search_address(self, query: str, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Search for addresses by text query.

        Args:
            query: Address search query
            limit: Maximum number of results to return

        Returns:
            List of address results with coordinates
        """
        try:
            url = f"{self.base_url}/adresser"
            params = {"q": query, "per_side": limit}

            data = self._make_request(url, params)

            if not data:
                return []

            results = []
            for address in data:
                # Extract coordinates from nested adgangsadresse structure
                adgangsadresse = address.get("adgangsadresse", {})
                coordinates = adgangsadresse.get("adgangspunkt", {}).get("koordinater", [])

                if len(coordinates) == 2:
                    # DAWA returns coordinates in WGS84 (SRID 4326) format
                    longitude, latitude = coordinates
                    results.append(
                        {
                            "adresse_id": address.get("id"),
                            "latitude": latitude,  # WGS84 latitude
                            "longitude": longitude,  # WGS84 longitude
                            "coordinate_system": "WGS84",  # Explicit coordinate system
                            "srid": 4326,  # EPSG code for WGS84
                            "full_address": address.get("adressebetegnelse"),
                            "street_name": adgangsadresse.get("vejstykke", {}).get("navn"),
                            "house_number": adgangsadresse.get("husnr"),
                            "floor": address.get("etage"),
                            "door": address.get("dør"),
                            "postal_code": adgangsadresse.get("postnummer", {}).get("nr"),
                            "city": adgangsadresse.get("postnummer", {}).get("navn"),
                            "municipality_code": adgangsadresse.get("kommune", {}).get("kode"),
                            "municipality_name": adgangsadresse.get("kommune", {}).get("navn"),
                            "coordinate_quality": adgangsadresse.get("adgangspunkt", {}).get(
                                "nøjagtighed"
                            ),
                            "coordinate_source": adgangsadresse.get("adgangspunkt", {}).get(
                                "kilde"
                            ),
                            "dawa_fetch_timestamp": time.time(),
                        }
                    )

            return results

        except Exception as e:
            self.log.error(f"Error searching for address '{query}': {e}")
            return []

    def create_geometry_wkt(self, latitude: float, longitude: float) -> str:
        """
        Create a WKT POINT geometry string from coordinates.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate

        Returns:
            WKT POINT string
        """
        return f"POINT({longitude} {latitude})"

    def create_geometry_geojson(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Create a GeoJSON Point geometry from coordinates.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate

        Returns:
            GeoJSON Point geometry
        """
        return {"type": "Point", "coordinates": [longitude, latitude]}

    def geocode_with_datavask(self, address_text: str) -> Optional[Dict[str, Any]]:
        """
        Geocode an address using the Datavask API as fallback.

        Datavask API can handle unstructured address text and correct spelling errors.
        It returns structured addresses with coordinates.

        Args:
            address_text: Unstructured address text

        Returns:
            Geocoded address data or None if not found
        """
        try:
            # Use Datavask API to clean and structure the address
            url = f"{self.base_url}/datavask/adresser"
            params = {"betegnelse": address_text, "format": "json"}

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not data.get("resultater") or len(data["resultater"]) == 0:
                self.log.debug(f"No Datavask results for address: {address_text}")
                return None

            # Get the best result (first one)
            result = data["resultater"][0]
            adresse = result.get("adresse", {})

            if not adresse:
                return None

            # Datavask returns structured address info, but we need to get coordinates
            # by calling the regular DAWA API with the address ID
            address_id = adresse.get("id")
            if address_id:
                # Get full address details including coordinates
                geocoded = self.geocode_address_by_id(address_id)
                if geocoded:
                    # Merge Datavask structure with DAWA coordinates
                    geocoded.update(
                        {
                            "floor": adresse.get("etage"),  # BFE: Floor from Datavask
                            "door": adresse.get("dør"),  # BFE: Door from Datavask
                            "datavask_enriched": True,
                            "dawa_enriched": True,  # Both APIs were used
                        }
                    )
                    return geocoded

            return None

        except Exception as e:
            self.log.error(f"Error geocoding address with Datavask '{address_text}': {e}")
            return None
