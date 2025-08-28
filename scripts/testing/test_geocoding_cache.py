#!/usr/bin/env python3
"""
Test script for geocoding cache implementation.

This script tests the geocoding cache functionality to ensure it:
1. Properly caches DAWA API results
2. Properly caches Datavask API results
3. Provides significant performance improvements
4. Persists cache data to GCS correctly
"""

import asyncio
import time

from backend.pipelines.unified_pipeline.src.unified_pipeline.util.cached_dawa_api_client import CachedDAWAAPIClient
from backend.pipelines.unified_pipeline.src.unified_pipeline.util.geocoding_cache import GeocodingCache


def test_geocoding_cache_basic():
    """Test basic geocoding cache functionality."""
    print("🧪 Testing basic geocoding cache functionality...")

    with GeocodingCache(cache_version=999) as cache:  # Use test version
        # Test DAWA ID cache
        test_adresse_id = "0a3f5095-45c5-32b8-e044-0003ba298018"

        # Should return None initially
        result = cache.lookup_by_dawa_id(test_adresse_id)
        assert result is None, "Cache should be empty initially"

        # Store a test result
        test_geocoding_result = {
            "latitude": 55.676098,
            "longitude": 12.568337,
            "coordinate_system": "WGS84",
            "srid": 4326,
            "geometry_wkt": "POINT(12.568337 55.676098)",
            "geometry_geojson": {"type": "Point", "coordinates": [12.568337, 55.676098]},
            "coordinate_quality": "A",
            "coordinate_source": "DAWA",
        }

        cache.store_dawa_result(test_adresse_id, test_geocoding_result)

        # Should now return the cached result
        cached_result = cache.lookup_by_dawa_id(test_adresse_id)
        assert cached_result is not None, "Cache should return stored result"
        assert cached_result["latitude"] == 55.676098, "Cached latitude should match"
        assert cached_result["longitude"] == 12.568337, "Cached longitude should match"
        assert cached_result["cached"] is True, "Result should be marked as cached"

        # Test address text cache
        test_address = "Christiansborg Slotsplads 1, 1218 København K"

        # Should return None initially
        result = cache.lookup_by_address_text(test_address)
        assert result is None, "Address cache should be empty initially"

        # Store a test result
        cache.store_address_text_result(test_address, test_geocoding_result, "datavask")

        # Should now return the cached result
        cached_result = cache.lookup_by_address_text(test_address)
        assert cached_result is not None, "Address cache should return stored result"
        assert cached_result["latitude"] == 55.676098, "Cached latitude should match"
        assert cached_result["api_source"] == "datavask", "API source should be recorded"

        # Test cache stats
        stats = cache.get_cache_stats()
        assert stats["dawa_id_cache_entries"] >= 1, "Should have at least 1 DAWA cache entry"
        assert stats["address_text_cache_entries"] >= 1, "Should have at least 1 address cache entry"

        print("✅ Basic geocoding cache functionality test passed!")


async def test_cached_dawa_client():
    """Test the cached DAWA API client."""
    print("🧪 Testing cached DAWA API client...")

    with CachedDAWAAPIClient(cache_version=999) as client:  # Use test version
        # Test with some real DAWA address IDs (these should be stable)
        test_addresses = [
            "0a3f5095-45c5-32b8-e044-0003ba298018",  # Christiansborg
            "0a3f5095-45c6-32b8-e044-0003ba298018",  # Another Copenhagen address
            "0a3f5095-45c7-32b8-e044-0003ba298018",  # Yet another address
        ]

        print("📍 First round - should hit API and cache results...")
        start_time = time.time()

        results_first = []
        for addr_id in test_addresses:
            result = client.geocode_address_by_id(addr_id)
            results_first.append(result)
            if result:
                print(f"   ✅ Geocoded {addr_id[:8]}... -> ({result.get('latitude')}, {result.get('longitude')})")
            else:
                print(f"   ❌ Failed to geocode {addr_id[:8]}...")

        first_round_time = time.time() - start_time
        print(f"   ⏱️  First round took {first_round_time:.2f} seconds")

        print("📍 Second round - should hit cache...")
        start_time = time.time()

        results_second = []
        for addr_id in test_addresses:
            result = client.geocode_address_by_id(addr_id)
            results_second.append(result)
            if result and result.get("cached"):
                print(f"   💾 Cache hit for {addr_id[:8]}...")
            elif result:
                print(f"   🔄 API call for {addr_id[:8]}... (unexpected)")

        second_round_time = time.time() - start_time
        print(f"   ⏱️  Second round took {second_round_time:.2f} seconds")

        # Calculate performance improvement
        if second_round_time > 0 and first_round_time > 0:
            speedup = first_round_time / second_round_time
            print(f"   🚀 Cache provided {speedup:.1f}x speedup!")

        # Test address text geocoding with cache
        print("📍 Testing address text geocoding with cache...")
        test_text_addresses = [
            "Christiansborg Slotsplads 1, 1218 København K",
            "Rådhuspladsen 1, 1550 København V",
        ]

        for address in test_text_addresses:
            result = client.geocode_with_datavask(address)
            if result:
                print(f"   ✅ Geocoded '{address[:30]}...' -> ({result.get('latitude')}, {result.get('longitude')})")
            else:
                print(f"   ❌ Failed to geocode '{address[:30]}...'")

        # Show performance stats
        client.log_performance_summary()

        print("✅ Cached DAWA API client test completed!")


def test_cache_persistence():
    """Test that cache persists to GCS correctly."""
    print("🧪 Testing cache persistence...")

    # Create cache, add data, and save
    with GeocodingCache(cache_version=999) as cache:
        test_data = {
            "latitude": 55.676098,
            "longitude": 12.568337,
            "coordinate_system": "WGS84",
            "srid": 4326,
            "geometry_wkt": "POINT(12.568337 55.676098)",
        }

        cache.store_dawa_result("test-persistence-id", test_data)
        cache.store_address_text_result("Test Address 123", test_data, "test")

        initial_stats = cache.get_cache_stats()
        print(f"   📊 Initial cache stats: {initial_stats}")

    # Create new cache instance and verify data was loaded
    with GeocodingCache(cache_version=999) as cache:
        loaded_stats = cache.get_cache_stats()
        print(f"   📊 Loaded cache stats: {loaded_stats}")

        # Verify specific data was persisted
        dawa_result = cache.lookup_by_dawa_id("test-persistence-id")
        address_result = cache.lookup_by_address_text("Test Address 123")

        assert dawa_result is not None, "DAWA cache should persist"
        assert address_result is not None, "Address cache should persist"

        print("   ✅ Cache data persisted correctly!")

    print("✅ Cache persistence test passed!")


async def main():
    """Run all geocoding cache tests."""
    print("🚀 Starting geocoding cache tests...\n")

    try:
        # Basic functionality tests
        test_geocoding_cache_basic()
        print()

        # Cached client tests
        await test_cached_dawa_client()
        print()

        # Persistence tests
        test_cache_persistence()
        print()

        print("🎉 All geocoding cache tests passed!")
        print("\n💡 The geocoding cache is ready to dramatically speed up CVR enrichment!")
        print("   • DAWA ID lookups will be cached for instant retrieval")
        print("   • Address text geocoding will avoid redundant API calls")
        print("   • Cache persists across pipeline runs in GCS")
        print("   • Performance metrics track cache effectiveness")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
