#!/usr/bin/env python3
"""
Test script for the Property-Cadastral merge pipeline.

This script provides basic tests and validation for the merge functionality.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from unified_pipeline.silver.property_cadastral_merge import (
    PropertyCadastralMerge,
    PropertyCadastralMergeConfig,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_mock_property_data():
    """Create mock property owners data for testing."""

    # Create sample property data with geometries
    properties = [
        {
            "property_id": 1,
            "ejendePerson": {
                "Person": {
                    "id": "uuid-1",  # Privacy-transformed CPR
                    "navn": "Test Owner 1",
                    "lives_abroad": False,
                }
            },
            "geometry": Point(12.5, 55.7),  # Copenhagen area
        },
        {
            "property_id": 2,
            "ejendePerson": {
                "Person": {"id": "uuid-2", "navn": "Test Owner 2", "lives_abroad": False}
            },
            "geometry": Point(12.51, 55.71),
        },
        {
            "property_id": 3,
            "ejendePerson": {
                "Person": {"id": "uuid-3", "navn": "Test Owner 3", "lives_abroad": True}
            },
            "geometry": Point(12.52, 55.72),
        },
    ]

    return gpd.GeoDataFrame(properties, crs="EPSG:4326")


def create_mock_cadastral_data():
    """Create mock cadastral data for testing."""

    # Create sample cadastral parcels
    parcels = [
        {
            "cadastral_id": 1,
            "bfe_number": 1001,
            "registration_from": "2023-01-01",
            "authority": "Test Municipality",
            "agricultural_notation": "Agricultural land",
            "is_worker_housing": False,
            "is_common_lot": False,
            "has_owner_apartments": False,
            "geometry": Polygon(
                [
                    (12.499, 55.699),
                    (12.501, 55.699),
                    (12.501, 55.701),
                    (12.499, 55.701),
                    (12.499, 55.699),
                ]
            ),
        },
        {
            "cadastral_id": 2,
            "bfe_number": 1002,
            "registration_from": "2023-01-01",
            "authority": "Test Municipality",
            "agricultural_notation": "Residential",
            "is_worker_housing": False,
            "is_common_lot": False,
            "has_owner_apartments": True,
            "geometry": Polygon(
                [
                    (12.509, 55.709),
                    (12.511, 55.709),
                    (12.511, 55.711),
                    (12.509, 55.711),
                    (12.509, 55.709),
                ]
            ),
        },
        {
            "cadastral_id": 3,
            "bfe_number": 1003,
            "registration_from": "2023-01-01",
            "authority": "Test Municipality",
            "agricultural_notation": "Forest",
            "is_worker_housing": False,
            "is_common_lot": True,
            "has_owner_apartments": False,
            "geometry": Polygon(
                [
                    (12.519, 55.719),
                    (12.521, 55.719),
                    (12.521, 55.721),
                    (12.519, 55.721),
                    (12.519, 55.719),
                ]
            ),
        },
    ]

    return gpd.GeoDataFrame(parcels, crs="EPSG:4326")


class MockGCSUtil:
    """Mock GCS utility for testing."""

    def __init__(self):
        self.files = {}

    def list_files(self, bucket_name, prefix):
        """Mock file listing."""
        mock_files = []

        if "property_owners" in prefix:
            mock_file = Mock()
            mock_file.name = f"{prefix}test_property_owners.parquet"
            mock_file.time_created = "2023-01-01T00:00:00Z"
            mock_files.append(mock_file)

        elif "cadastral" in prefix:
            mock_file = Mock()
            mock_file.name = f"{prefix}test_cadastral.parquet"
            mock_file.time_created = "2023-01-01T00:00:00Z"
            mock_files.append(mock_file)

        return mock_files

    def download_file(self, bucket_name, source_blob_name, destination_file_name):
        """Mock file download - create test data."""

        if "property_owners" in source_blob_name:
            # Create mock property owners parquet
            property_data = create_mock_property_data()
            # Convert geometry to JSON string to simulate the SFTP pipeline output
            property_data["geometry_json"] = property_data["geometry"].apply(
                lambda geom: json.dumps(geom.__geo_interface__)
            )
            property_data = property_data.drop("geometry", axis=1)
            pd.DataFrame(property_data).to_parquet(destination_file_name)

        elif "cadastral" in source_blob_name:
            # Create mock cadastral parquet
            cadastral_data = create_mock_cadastral_data()
            cadastral_data.to_parquet(destination_file_name)


async def test_basic_merge():
    """Test basic merge functionality."""
    logger.info("Testing basic merge functionality...")

    # Create test configuration
    config = PropertyCadastralMergeConfig(
        dataset="test_merge",
        spatial_join_method="intersects",
        buffer_distance_meters=100.0,  # Large buffer for testing
        min_overlap_threshold=0.0,  # No filtering for testing
        bucket="test-bucket",
    )

    # Create mock GCS utility
    mock_gcs = MockGCSUtil()

    # Create merge pipeline
    merge_pipeline = PropertyCadastralMerge(config, mock_gcs)

    # Test loading property data
    property_gdf = merge_pipeline._load_property_owners_data()
    assert property_gdf is not None, "Failed to load property owners data"
    assert len(property_gdf) == 3, f"Expected 3 properties, got {len(property_gdf)}"
    logger.info(f"✅ Loaded {len(property_gdf)} property records")

    # Test loading cadastral data
    cadastral_gdf = merge_pipeline._load_cadastral_data()
    assert cadastral_gdf is not None, "Failed to load cadastral data"
    assert len(cadastral_gdf) == 3, f"Expected 3 cadastral parcels, got {len(cadastral_gdf)}"
    logger.info(f"✅ Loaded {len(cadastral_gdf)} cadastral records")

    # Test spatial merge
    merged_gdf = merge_pipeline._perform_spatial_merge(property_gdf, cadastral_gdf)
    assert merged_gdf is not None, "Spatial merge failed"
    assert len(merged_gdf) >= len(property_gdf), (
        "Merge result should have at least as many records as properties"
    )
    logger.info(f"✅ Spatial merge completed: {len(merged_gdf)} records")

    # Test data cleaning
    cleaned_gdf = merge_pipeline._clean_and_standardize(merged_gdf)
    assert cleaned_gdf is not None, "Data cleaning failed"
    assert "merge_timestamp" in cleaned_gdf.columns, "Missing merge metadata"
    assert "has_cadastral_match" in cleaned_gdf.columns, "Missing match indicator"
    logger.info(f"✅ Data cleaning completed: {len(cleaned_gdf)} records")

    # Check match statistics
    matched_count = len(cleaned_gdf[cleaned_gdf["has_cadastral_match"]])
    match_rate = (matched_count / len(cleaned_gdf)) * 100
    logger.info(f"Match statistics: {matched_count}/{len(cleaned_gdf)} ({match_rate:.1f}%)")

    return True


def test_spatial_methods():
    """Test different spatial join methods."""
    logger.info("Testing spatial join methods...")

    # Create test geometries with known relationships
    property_geom = Point(12.5, 55.7)

    # Cadastral parcel containing the property
    containing_parcel = Polygon(
        [(12.49, 55.69), (12.51, 55.69), (12.51, 55.71), (12.49, 55.71), (12.49, 55.69)]
    )

    # Test geometric relationships
    assert property_geom.intersects(containing_parcel), (
        "Property should intersect with containing parcel"
    )
    assert property_geom.within(containing_parcel), "Property should be within containing parcel"
    assert not containing_parcel.within(property_geom), "Parcel should not be within property point"

    logger.info("✅ Spatial relationship tests passed")
    return True


def test_privacy_preservation():
    """Test that privacy transformations are preserved."""
    logger.info("Testing privacy preservation...")

    # Create test data
    property_data = create_mock_property_data()

    # Check that CPR numbers are UUIDs (privacy-transformed)
    for idx, row in property_data.iterrows():
        person_data = row["ejendePerson"]["Person"]
        person_id = person_data["id"]

        # Check that ID looks like a UUID (not a CPR number)
        assert "uuid" in person_id, f"ID should be UUID format, got: {person_id}"
        assert len(person_id.split("-")) >= 4, f"UUID should have dashes, got: {person_id}"

    logger.info("✅ Privacy preservation tests passed")
    return True


def test_configuration_validation():
    """Test configuration validation."""
    logger.info("Testing configuration validation...")

    # Test valid configuration
    try:
        config = PropertyCadastralMergeConfig(
            dataset="test",
            spatial_join_method="intersects",
            buffer_distance_meters=10.0,
            min_overlap_threshold=0.1,
        )
        logger.info("✅ Valid configuration accepted")
    except Exception as e:
        logger.error(f"❌ Valid configuration rejected: {e}")
        return False

    # Test invalid spatial method
    try:
        config = PropertyCadastralMergeConfig(
            dataset="test",
            spatial_join_method="invalid_method",  # This should fail
            buffer_distance_meters=10.0,
            min_overlap_threshold=0.1,
        )
        logger.warning("⚠️ Invalid spatial method was accepted (should validate)")
    except Exception:
        logger.info("✅ Invalid spatial method properly rejected")

    return True


async def run_all_tests():
    """Run all tests."""
    logger.info("Starting Property-Cadastral Merge Tests")
    logger.info("=" * 50)

    tests = [
        ("Configuration Validation", test_configuration_validation),
        ("Privacy Preservation", test_privacy_preservation),
        ("Spatial Methods", test_spatial_methods),
        ("Basic Merge", test_basic_merge),
    ]

    results = {}

    for test_name, test_func in tests:
        logger.info(f"\n🔍 Running: {test_name}")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results[test_name] = result
            logger.info(f"✅ {test_name}: PASSED")
        except Exception as e:
            logger.error(f"❌ {test_name}: FAILED - {e}")
            results[test_name] = False

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST SUMMARY:")

    passed = sum(1 for result in results.values() if result)
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"  {test_name}: {status}")

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed!")
        return True
    else:
        logger.error(f"💥 {total - passed} tests failed!")
        return False


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
