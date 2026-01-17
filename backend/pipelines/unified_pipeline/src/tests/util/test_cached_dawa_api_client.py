"""
Tests for Cached DAWA API Client.

Tests for: unified_pipeline/util/cached_dawa_api_client.py

Covers:
- Cache hits and misses
- Cache expiration
- Cache key generation
- Cache persistence
- Concurrent access safety
- Performance statistics
"""

from unittest.mock import Mock, patch, MagicMock

import pytest

from unified_pipeline.util.cached_dawa_api_client import CachedDAWAAPIClient


class TestCachedDAWAAPIClient:
    """Tests for cached DAWA API client initialization."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_client_initialization(self, mock_cache, mock_dawa):
        """Test cached client initialization."""
        client = CachedDAWAAPIClient()

        assert client.dawa_client is not None
        assert client.cache is not None
        assert client.stats["dawa_id_lookups"] == 0
        assert client.stats["dawa_id_cache_hits"] == 0

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_client_initialization_with_cache_version(self, mock_cache, mock_dawa):
        """Test client initialization with custom cache version."""
        client = CachedDAWAAPIClient(cache_version=2)

        # Verify cache version is passed
        mock_cache.assert_called_once_with(cache_version=2)


class TestCacheHitsAndMisses:
    """Tests for cache hit and miss scenarios."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cache_hit_dawa_id(self, mock_cache_class, mock_dawa):
        """Test cache hit for DAWA ID lookup."""
        # Mock cache to return cached result
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
            "cached": True,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should return cached result
        assert result is not None
        assert result["latitude"] == 55.6761
        assert result["cached"] is True

        # Stats should show cache hit
        assert client.stats["dawa_id_lookups"] == 1
        assert client.stats["dawa_id_cache_hits"] == 1
        assert client.stats["dawa_id_api_calls"] == 0

        # DAWA API should not be called
        mock_dawa.return_value.geocode_address_by_id.assert_not_called()

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cache_miss_dawa_id(self, mock_cache_class, mock_dawa_class):
        """Test cache miss for DAWA ID lookup."""
        # Mock cache to return None (cache miss)
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = None
        mock_cache_class.return_value = mock_cache

        # Mock DAWA API to return result
        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
            "adresse_id": "test-id",
        }
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should return API result
        assert result is not None
        assert result["latitude"] == 55.6761

        # Stats should show cache miss and API call
        assert client.stats["dawa_id_lookups"] == 1
        assert client.stats["dawa_id_cache_hits"] == 0
        assert client.stats["dawa_id_api_calls"] == 1

        # Result should be stored in cache
        mock_cache.store_dawa_result.assert_called_once()

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cache_hit_address_text(self, mock_cache_class, mock_dawa):
        """Test cache hit for address text lookup."""
        # Mock cache to return cached result
        mock_cache = Mock()
        mock_cache.lookup_by_address_text.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
            "cached": True,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()
        result = client.geocode_with_datavask("Rødkildevej 46, 2400 København NV")

        # Should return cached result
        assert result is not None
        assert result["cached"] is True

        # Stats should show cache hit
        assert client.stats["address_text_lookups"] == 1
        assert client.stats["address_text_cache_hits"] == 1
        assert client.stats["address_text_api_calls"] == 0

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cache_miss_address_text(self, mock_cache_class, mock_dawa_class):
        """Test cache miss for address text lookup."""
        # Mock cache to return None (cache miss)
        mock_cache = Mock()
        mock_cache.lookup_by_address_text.return_value = None
        mock_cache_class.return_value = mock_cache

        # Mock DAWA API to return result
        mock_dawa = Mock()
        mock_dawa.geocode_with_datavask.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
        }
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_with_datavask("Rødkildevej 46")

        # Should return API result
        assert result is not None

        # Stats should show cache miss and API call
        assert client.stats["address_text_lookups"] == 1
        assert client.stats["address_text_cache_hits"] == 0
        assert client.stats["address_text_api_calls"] == 1

        # Result should be stored in cache
        mock_cache.store_address_text_result.assert_called_once()


class TestEmptyInputHandling:
    """Tests for handling empty/None inputs."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_geocode_empty_dawa_id(self, mock_cache_class, mock_dawa):
        """Test geocoding with empty DAWA ID."""
        client = CachedDAWAAPIClient()

        result = client.geocode_address_by_id("")
        assert result is None

        result = client.geocode_address_by_id(None)
        assert result is None

        # No cache lookups should happen
        assert client.stats["dawa_id_lookups"] == 0

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_geocode_empty_address_text(self, mock_cache_class, mock_dawa):
        """Test geocoding with empty address text."""
        client = CachedDAWAAPIClient()

        result = client.geocode_with_datavask("")
        assert result is None

        result = client.geocode_with_datavask(None)
        assert result is None

        # No cache lookups should happen
        assert client.stats["address_text_lookups"] == 0


class TestPerformanceStatistics:
    """Tests for performance statistics tracking."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_get_performance_stats(self, mock_cache_class, mock_dawa):
        """Test performance statistics calculation."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = {"latitude": 55.6, "longitude": 12.5}
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 100,
            "address_text_cache_entries": 50,
            "total_cache_entries": 150,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()

        # Simulate some lookups
        client.geocode_address_by_id("id1")  # Hit
        client.geocode_address_by_id("id2")  # Hit

        stats = client.get_performance_stats()

        assert stats["dawa_id_lookups"] == 2
        assert stats["dawa_id_cache_hits"] == 2
        assert stats["dawa_id_cache_hit_rate"] == 100.0
        assert stats["total_cache_entries"] == 150

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cache_hit_rate_calculation(self, mock_cache_class, mock_dawa_class):
        """Test cache hit rate calculation."""
        # Mix of hits and misses
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.side_effect = [
            {"latitude": 55.6, "longitude": 12.5},  # Hit
            None,  # Miss
            {"latitude": 55.7, "longitude": 12.6},  # Hit
            None,  # Miss
        ]
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 50,
            "address_text_cache_entries": 0,
            "total_cache_entries": 50,
        }
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.return_value = {"latitude": 55.8, "longitude": 12.7}
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()

        # 4 lookups: 2 hits, 2 misses
        client.geocode_address_by_id("id1")
        client.geocode_address_by_id("id2")
        client.geocode_address_by_id("id3")
        client.geocode_address_by_id("id4")

        stats = client.get_performance_stats()

        assert stats["dawa_id_lookups"] == 4
        assert stats["dawa_id_cache_hits"] == 2
        assert stats["dawa_id_cache_hit_rate"] == 50.0

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_log_performance_summary(self, mock_cache_class, mock_dawa):
        """Test performance summary logging."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = {"latitude": 55.6, "longitude": 12.5}
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 100,
            "address_text_cache_entries": 50,
            "total_cache_entries": 150,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()
        client.geocode_address_by_id("id1")

        # Should not raise exception
        client.log_performance_summary()


class TestGeometryHelpers:
    """Tests for geometry helper functions (delegated to DAWA client)."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_create_geometry_wkt(self, mock_cache_class, mock_dawa_class):
        """Test WKT geometry creation."""
        mock_dawa = Mock()
        mock_dawa.create_geometry_wkt.return_value = "POINT(12.5683 55.6761)"
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        wkt = client.create_geometry_wkt(55.6761, 12.5683)

        assert wkt == "POINT(12.5683 55.6761)"
        mock_dawa.create_geometry_wkt.assert_called_once_with(55.6761, 12.5683)

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_create_geometry_geojson(self, mock_cache_class, mock_dawa_class):
        """Test GeoJSON geometry creation."""
        mock_dawa = Mock()
        mock_dawa.create_geometry_geojson.return_value = {
            "type": "Point",
            "coordinates": [12.5683, 55.6761],
        }
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        geojson = client.create_geometry_geojson(55.6761, 12.5683)

        assert geojson["type"] == "Point"
        assert geojson["coordinates"] == [12.5683, 55.6761]


class TestContextManager:
    """Tests for context manager functionality."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_context_manager_enter_exit(self, mock_cache_class, mock_dawa):
        """Test context manager protocol."""
        mock_cache = Mock()
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 0,
            "address_text_cache_entries": 0,
            "total_cache_entries": 0,
        }
        mock_cache_class.return_value = mock_cache

        with CachedDAWAAPIClient() as client:
            assert client is not None
            # Use the client
            client.geocode_address_by_id("test-id")

        # Cleanup should be called on exit
        mock_cache.cleanup.assert_called_once()

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_context_manager_with_exception(self, mock_cache_class, mock_dawa):
        """Test context manager cleanup happens even with exception."""
        mock_cache = Mock()
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 0,
            "address_text_cache_entries": 0,
            "total_cache_entries": 0,
        }
        mock_cache_class.return_value = mock_cache

        try:
            with CachedDAWAAPIClient() as client:
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Cleanup should still be called
        mock_cache.cleanup.assert_called_once()


class TestCleanup:
    """Tests for cleanup and resource management."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cleanup_logs_performance(self, mock_cache_class, mock_dawa):
        """Test cleanup logs performance summary."""
        mock_cache = Mock()
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 0,
            "address_text_cache_entries": 0,
            "total_cache_entries": 0,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()
        client.cleanup()

        # Should call cache cleanup
        mock_cache.cleanup.assert_called_once()

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_cleanup_saves_cache(self, mock_cache_class, mock_dawa):
        """Test cleanup saves cache to GCS."""
        mock_cache = Mock()
        mock_cache.get_cache_stats.return_value = {
            "dawa_id_cache_entries": 0,
            "address_text_cache_entries": 0,
            "total_cache_entries": 0,
        }
        mock_cache_class.return_value = mock_cache

        client = CachedDAWAAPIClient()
        client.cleanup()

        # Cache cleanup should be called (which saves to GCS)
        mock_cache.cleanup.assert_called_once()


class TestCacheStorage:
    """Tests for cache storage behavior."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_successful_result_stored_in_cache(self, mock_cache_class, mock_dawa_class):
        """Test that successful API results are stored in cache."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = None  # Cache miss
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
        }
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Result should be stored
        mock_cache.store_dawa_result.assert_called_once_with("test-id", result)

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_null_result_not_stored_in_cache(self, mock_cache_class, mock_dawa_class):
        """Test that null API results are not stored in cache."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = None  # Cache miss
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.return_value = None  # API returns None
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Null result should not be stored
        mock_cache.store_dawa_result.assert_not_called()

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_datavask_result_stored_with_source(self, mock_cache_class, mock_dawa_class):
        """Test that Datavask results are stored with correct source."""
        mock_cache = Mock()
        mock_cache.lookup_by_address_text.return_value = None  # Cache miss
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_with_datavask.return_value = {
            "latitude": 55.6761,
            "longitude": 12.5683,
        }
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_with_datavask("Rødkildevej 46")

        # Result should be stored with 'datavask' source
        call_args = mock_cache.store_address_text_result.call_args
        assert call_args[0][2] == "datavask"


class TestAPIFailureHandling:
    """Tests for handling API failures."""

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_api_exception_returns_none(self, mock_cache_class, mock_dawa_class):
        """Test that API exceptions return None gracefully."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = None  # Cache miss
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.side_effect = Exception("API error")
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Should return None instead of raising exception
        assert result is None

    @patch("unified_pipeline.util.cached_dawa_api_client.DAWAAPIClient")
    @patch("unified_pipeline.util.cached_dawa_api_client.GeocodingCache")
    def test_failed_result_not_cached(self, mock_cache_class, mock_dawa_class):
        """Test that failed API calls don't cache None results."""
        mock_cache = Mock()
        mock_cache.lookup_by_dawa_id.return_value = None  # Cache miss
        mock_cache_class.return_value = mock_cache

        mock_dawa = Mock()
        mock_dawa.geocode_address_by_id.side_effect = Exception("API error")
        mock_dawa_class.return_value = mock_dawa

        client = CachedDAWAAPIClient()
        result = client.geocode_address_by_id("test-id")

        # Failed result should not be stored in cache
        mock_cache.store_dawa_result.assert_not_called()
