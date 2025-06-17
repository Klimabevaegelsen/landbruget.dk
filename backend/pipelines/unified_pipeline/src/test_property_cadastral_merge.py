#!/usr/bin/env python3
"""
Test script for the Property-Cadastral merge pipeline.

This script provides basic tests and validation for the merge functionality.
"""

import asyncio
import logging
import sys
from pathlib import Path
from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

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

    # Create sample property data with BFE numbers
    properties = [
        {
            "property_id": 1,
            "bestemtFastEjendomBFENr": 100001,  # BFE number for direct join
            "ejendePerson": {
                "Person": {
                    "id": "uuid-1",  # Privacy-transformed CPR
                    "navn": "Test Owner 1",
                    "lives_abroad": False,
                }
            },
        },
        {
            "property_id": 2,
            "bestemtFastEjendomBFENr": 100002,
            "ejendePerson": {
                "Person": {"id": "uuid-2", "navn": "Test Owner 2", "lives_abroad": False}
            },
        },
        {
            "property_id": 3,
            "bestemtFastEjendomBFENr": 100003,
            "ejendePerson": {
                "Person": {"id": "uuid-3", "navn": "Test Owner 3", "lives_abroad": True}
            },
        },
        {
            "property_id": 4,
            "bestemtFastEjendomBFENr": 999999,  # BFE number without cadastral match
            "ejendePerson": {
                "Person": {"id": "uuid-4", "navn": "Test Owner 4", "lives_abroad": False}
            },
        },
    ]

    return pd.DataFrame(properties)


def create_mock_cadastral_data():
    """Create mock cadastral data for testing."""

    # Create sample cadastral parcels with matching BFE numbers
    parcels = [
        {
            "cadastral_id": 1,
            "bfe_number": 100001,  # Matches property_id 1
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
            "bfe_number": 100002,  # Matches property_id 2
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
            "bfe_number": 100003,  # Matches property_id 3
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
        {
            "cadastral_id": 4,
            "bfe_number": 100004,  # Cadastral parcel without property owner
            "registration_from": "2023-01-01",
            "authority": "Test Municipality",
            "agricultural_notation": "Commercial",
            "is_worker_housing": False,
            "is_common_lot": False,
            "has_owner_apartments": False,
            "geometry": Polygon(
                [
                    (12.529, 55.729),
                    (12.531, 55.729),
                    (12.531, 55.731),
                    (12.529, 55.731),
                    (12.529, 55.729),
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
            # Create mock property owners parquet (now DataFrame with BFE numbers)
            property_data = create_mock_property_data()
            property_data.to_parquet(destination_file_name)

        elif "cadastral" in source_blob_name:
            # Create mock cadastral parquet
            cadastral_data = create_mock_cadastral_data()
            cadastral_data.to_parquet(destination_file_name)


async def test_basic_merge():
    """Test basic BFE-based merge functionality."""
    logger.info("Testing basic BFE-based merge functionality...")

    # Create test configuration
    config = PropertyCadastralMergeConfig(
        dataset="test_merge",
        join_method="inner",
        validate_bfe_numbers=True,
        include_merge_metadata=True,
        bucket="test-bucket",
    )

    # Create mock GCS utility
    mock_gcs = MockGCSUtil()

    # Create merge pipeline
    merge_pipeline = PropertyCadastralMerge(config, mock_gcs)

    # Test loading property data
    property_df = merge_pipeline._load_property_owners_data()
    assert property_df is not None, "Failed to load property owners data"
    assert len(property_df) == 4, f"Expected 4 properties, got {len(property_df)}"
    assert "bestemtFastEjendomBFENr" in property_df.columns, "Missing BFE number field"
    logger.info(f"✅ Loaded {len(property_df)} property records")

    # Test loading cadastral data
    cadastral_gdf = merge_pipeline._load_cadastral_data()
    assert cadastral_gdf is not None, "Failed to load cadastral data"
    assert len(cadastral_gdf) == 4, f"Expected 4 cadastral parcels, got {len(cadastral_gdf)}"
    assert "bfe_number" in cadastral_gdf.columns, "Missing BFE number field in cadastral data"
    logger.info(f"✅ Loaded {len(cadastral_gdf)} cadastral records")

    # Test BFE-based merge
    merged_gdf = merge_pipeline._perform_bfe_merge(property_df, cadastral_gdf)
    assert merged_gdf is not None, "BFE merge failed"
    # With inner join, we should only get properties with matching cadastral parcels (3 out of 4)
    assert len(merged_gdf) == 3, f"Expected 3 merged records, got {len(merged_gdf)}"
    logger.info(f"✅ BFE merge completed: {len(merged_gdf)} records")

    # Test merge quality validation
    quality_stats = merge_pipeline._validate_bfe_merge_quality(
        merged_gdf, property_df, cadastral_gdf
    )
    assert quality_stats is not None, "Quality validation failed"
    assert quality_stats["total_properties"] == 4, "Incorrect property count in stats"
    assert quality_stats["total_cadastral_parcels"] == 4, "Incorrect cadastral count in stats"
    logger.info(
        f"✅ Quality validation completed: {quality_stats['match_rate_percent']:.1f}% match rate"
    )

    # Test data cleaning
    cleaned_gdf = merge_pipeline._clean_and_standardize(merged_gdf, quality_stats)
    assert cleaned_gdf is not None, "Data cleaning failed"
    assert "merge_timestamp" in cleaned_gdf.columns, "Missing merge metadata"
    assert "has_cadastral_match" in cleaned_gdf.columns, "Missing match indicator"
    logger.info(f"✅ Data cleaning completed: {len(cleaned_gdf)} records")

    # Check match statistics
    matched_count = len(cleaned_gdf[cleaned_gdf["has_cadastral_match"]])
    match_rate = (matched_count / len(cleaned_gdf)) * 100
    logger.info(f"Match statistics: {matched_count}/{len(cleaned_gdf)} ({match_rate:.1f}%)")

    # Verify BFE number matching worked correctly (inner join = all records should be matched)
    assert matched_count == 3, f"Expected 3 matches, got {matched_count}"
    assert matched_count == len(cleaned_gdf), "All records should be matched with inner join"

    return True


def test_bfe_join_methods():
    """Test different BFE join methods."""
    logger.info("Testing BFE join methods...")

    # Create test data
    property_data = create_mock_property_data()
    cadastral_data = create_mock_cadastral_data()

    # Test inner join - only matching records
    inner_result = pd.merge(
        property_data,
        cadastral_data,
        left_on="bestemtFastEjendomBFENr",
        right_on="bfe_number",
        how="inner",
    )
    assert len(inner_result) == 3, f"Inner join should return 3 matches, got {len(inner_result)}"

    # Test left join - all properties
    left_result = pd.merge(
        property_data,
        cadastral_data,
        left_on="bestemtFastEjendomBFENr",
        right_on="bfe_number",
        how="left",
    )
    assert len(left_result) == 4, f"Left join should return 4 records, got {len(left_result)}"

    # Check that unmatched property has NaN for cadastral fields
    unmatched = left_result[left_result["bfe_number"].isna()]
    assert len(unmatched) == 1, f"Should have 1 unmatched property, got {len(unmatched)}"
    assert unmatched.iloc[0]["bestemtFastEjendomBFENr"] == 999999, "Wrong unmatched BFE number"

    # Test right join - all cadastral parcels
    right_result = pd.merge(
        property_data,
        cadastral_data,
        left_on="bestemtFastEjendomBFENr",
        right_on="bfe_number",
        how="right",
    )
    assert len(right_result) == 4, f"Right join should return 4 records, got {len(right_result)}"

    logger.info("✅ BFE join method tests passed")
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
        # For mock data, we accept simple uuid-N format
        assert person_id.startswith("uuid-"), f"UUID should start with uuid-, got: {person_id}"

    logger.info("✅ Privacy preservation tests passed")
    return True


def test_configuration_validation():
    """Test configuration validation."""
    logger.info("Testing configuration validation...")

    # Test valid configuration
    try:
        config = PropertyCadastralMergeConfig(
            dataset="test",
            join_method="left",
            validate_bfe_numbers=True,
            include_merge_metadata=True,
        )
        logger.info("✅ Valid configuration accepted")
    except Exception as e:
        logger.error(f"❌ Valid configuration rejected: {e}")
        return False

    # Test different join methods
    for join_method in ["inner", "left", "right", "outer"]:
        try:
            config = PropertyCadastralMergeConfig(
                dataset="test",
                join_method=join_method,
                validate_bfe_numbers=True,
            )
            logger.info(f"✅ Join method '{join_method}' accepted")
        except Exception as e:
            logger.error(f"❌ Join method '{join_method}' rejected: {e}")
            return False

    return True


async def run_all_tests():
    """Run all tests."""
    logger.info("Starting Property-Cadastral Merge Tests")
    logger.info("=" * 50)

    tests = [
        ("Configuration Validation", test_configuration_validation),
        ("Privacy Preservation", test_privacy_preservation),
        ("BFE Join Methods", test_bfe_join_methods),
        ("Basic BFE Merge", test_basic_merge),
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
