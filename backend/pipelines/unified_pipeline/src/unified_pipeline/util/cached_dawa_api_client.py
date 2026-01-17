"""
Cached DAWA API Client
======================

Enhanced DAWA API client that integrates with the geocoding cache system
to avoid redundant API calls. Provides transparent caching for both
DAWA ID lookups and address text geocoding with Datavask fallback.

Features:
- Automatic cache lookup before API calls
- Transparent cache storage after successful API calls
- Compatible with existing DAWA API client interface
- Performance metrics and cache hit rate tracking
"""

from typing import Any

from unified_pipeline.util.dawa_api_client import DAWAAPIClient
from unified_pipeline.util.geocoding_cache import GeocodingCache
from unified_pipeline.util.log_util import Logger


class CachedDAWAAPIClient:
    """
    DAWA API client with integrated geocoding cache.

    Provides the same interface as DAWAAPIClient but with automatic
    caching to avoid redundant API calls.
    """

    def __init__(self, cache_version: int = 1):
        """
        Initialize cached DAWA API client.

        Args:
            cache_version: Cache version for invalidation
        """
        self.log = Logger.get_logger()
        self.dawa_client = DAWAAPIClient()
        self.cache = GeocodingCache(cache_version=cache_version)

        # Performance tracking
        self.stats = {
            "dawa_id_lookups": 0,
            "dawa_id_cache_hits": 0,
            "dawa_id_api_calls": 0,
            "address_text_lookups": 0,
            "address_text_cache_hits": 0,
            "address_text_api_calls": 0,
        }

        self.log.info("Cached DAWA API client initialized")

    def geocode_address_by_id(self, adresse_id: str) -> dict[str, Any] | None:
        """
        Geocode an address using its DAWA address ID with caching.

        Args:
            adresse_id: UUID address identifier from CVR

        Returns:
            Dictionary with address and coordinate information, or None if not found
        """
        if not adresse_id:
            return None

        self.stats["dawa_id_lookups"] += 1

        # Try cache first
        cached_result = self.cache.lookup_by_dawa_id(adresse_id)
        if cached_result:
            self.stats["dawa_id_cache_hits"] += 1
            self.log.debug(f"Cache hit for DAWA ID: {adresse_id}")
            return cached_result

        # Cache miss - call API
        self.stats["dawa_id_api_calls"] += 1
        self.log.debug(f"Cache miss for DAWA ID: {adresse_id} - calling API")

        try:
            result = self.dawa_client.geocode_address_by_id(adresse_id)

            if result:
                # Store in cache for future use
                self.cache.store_dawa_result(adresse_id, result)
                self.log.debug(f"Cached DAWA result for ID: {adresse_id}")

            return result

        except Exception as e:
            self.log.warning(f"DAWA API call failed for ID {adresse_id}: {e}")
            return None

    def geocode_with_datavask(self, address: str) -> dict[str, Any] | None:
        """
        Geocode an address using Datavask API with caching.

        Args:
            address: Full address string

        Returns:
            Dictionary with address and coordinate information, or None if not found
        """
        if not address:
            return None

        self.stats["address_text_lookups"] += 1

        # Try cache first
        cached_result = self.cache.lookup_by_address_text(address)
        if cached_result:
            self.stats["address_text_cache_hits"] += 1
            self.log.debug(f"Cache hit for address: {address[:50]}...")
            return cached_result

        # Cache miss - call API
        self.stats["address_text_api_calls"] += 1
        self.log.debug(f"Cache miss for address: {address[:50]}... - calling API")

        try:
            result = self.dawa_client.geocode_with_datavask(address)

            if result:
                # Store in cache for future use
                self.cache.store_address_text_result(address, result, "datavask")
                self.log.debug(f"Cached Datavask result for address: {address[:50]}...")

            return result

        except Exception as e:
            self.log.warning(f"Datavask API call failed for address {address}: {e}")
            return None

    def create_geometry_wkt(self, latitude: float, longitude: float) -> str:
        """Create WKT geometry from coordinates (delegated to DAWA client)."""
        return self.dawa_client.create_geometry_wkt(latitude, longitude)

    def create_geometry_geojson(self, latitude: float, longitude: float) -> dict[str, Any]:
        """Create GeoJSON geometry from coordinates (delegated to DAWA client)."""
        return self.dawa_client.create_geometry_geojson(latitude, longitude)

    def get_performance_stats(self) -> dict[str, Any]:
        """
        Get performance statistics including cache hit rates.

        Returns:
            Dictionary with performance metrics
        """
        dawa_hit_rate = (
            self.stats["dawa_id_cache_hits"] / max(self.stats["dawa_id_lookups"], 1) * 100
        )
        address_hit_rate = (
            self.stats["address_text_cache_hits"] / max(self.stats["address_text_lookups"], 1) * 100
        )
        overall_hit_rate = (
            (self.stats["dawa_id_cache_hits"] + self.stats["address_text_cache_hits"])
            / max(self.stats["dawa_id_lookups"] + self.stats["address_text_lookups"], 1)
            * 100
        )

        cache_stats = self.cache.get_cache_stats()

        return {
            **self.stats,
            "dawa_id_cache_hit_rate": round(dawa_hit_rate, 2),
            "address_text_cache_hit_rate": round(address_hit_rate, 2),
            "overall_cache_hit_rate": round(overall_hit_rate, 2),
            **cache_stats,
        }

    def log_performance_summary(self) -> None:
        """Log a summary of cache performance."""
        stats = self.get_performance_stats()

        self.log.info("🚀 Geocoding Cache Performance Summary:")
        self.log.info(f"   • DAWA ID lookups: {stats['dawa_id_lookups']:,}")
        self.log.info(
            f"   • DAWA ID cache hits: {stats['dawa_id_cache_hits']:,} "
            f"({stats['dawa_id_cache_hit_rate']:.1f}%)"
        )
        self.log.info(f"   • DAWA ID API calls: {stats['dawa_id_api_calls']:,}")
        self.log.info(f"   • Address text lookups: {stats['address_text_lookups']:,}")
        self.log.info(
            f"   • Address text cache hits: {stats['address_text_cache_hits']:,} "
            f"({stats['address_text_cache_hit_rate']:.1f}%)"
        )
        self.log.info(f"   • Address text API calls: {stats['address_text_api_calls']:,}")
        self.log.info(f"   • Overall cache hit rate: {stats['overall_cache_hit_rate']:.1f}%")
        self.log.info(f"   • Total cache entries: {stats['total_cache_entries']:,}")

        # Calculate API call savings
        total_api_calls = stats["dawa_id_api_calls"] + stats["address_text_api_calls"]
        total_cache_hits = stats["dawa_id_cache_hits"] + stats["address_text_cache_hits"]
        api_calls_saved = total_cache_hits

        if total_api_calls + total_cache_hits > 0:
            savings_percent = (api_calls_saved / (total_api_calls + total_cache_hits)) * 100
            self.log.info(
                f"   • API calls saved: {api_calls_saved:,} ({savings_percent:.1f}% reduction)"
            )

    def cleanup(self) -> None:
        """Clean up resources and save cache."""
        self.log_performance_summary()
        self.cache.cleanup()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup and save cache."""
        self.cleanup()
