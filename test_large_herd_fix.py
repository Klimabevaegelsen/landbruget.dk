#!/usr/bin/env python3
"""
Test script to verify that large herds are no longer being skipped.
This tests the fix for GitHub issue #387: cattle transport skipping some CHRs.
"""

import logging
import sys
from unittest.mock import Mock

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def test_large_herd_processing():
    """Test that large herds (>100k animals) are processed instead of skipped."""

    # Import the fixed function
    sys.path.insert(0, "backend/pipelines/chr_pipeline")
    from bronze.data_processing import process_chr_dyr_animals

    logger.info("🧪 Testing large herd processing fix...")

    # Mock a large dataset (>100k animals)
    large_animals_data = []
    animals_count = 150000  # This would previously be skipped

    # Create mock animal data
    for i in range(10):  # Just a sample for testing
        large_animals_data.append(
            {
                "CkrNr": f"12345{i}",
                "DatoAfgaaet": "2024-01-15",
                "KildeBesaetning": "12345",
                "DestinationBesaetning": "54321",
                "Koen": "K",
            }
        )

    # Mock the animals list to appear as if it has 150k animals
    # but only process a small sample to avoid actual processing overhead
    mock_animals_with_count = Mock()
    mock_animals_with_count.__len__ = Mock(return_value=animals_count)
    mock_animals_with_count.__iter__ = Mock(return_value=iter(large_animals_data))
    mock_animals_with_count.__bool__ = Mock(return_value=True)

    logger.info(f"📊 Testing with simulated herd of {animals_count:,} animals...")

    # Test the function
    result = process_chr_dyr_animals(
        reporting_herd=112389,  # Known large herd
        animals=mock_animals_with_count,
    )

    # Check if the function processed the data instead of skipping it
    if result is None:
        logger.error("❌ FAIL: Function returned None - this indicates an error")
        return False

    # Check if the result indicates the herd was skipped
    if result.get("skipped_reason") == "dataset_too_large":
        logger.error("❌ FAIL: Large herd was still skipped due to 'dataset_too_large'")
        return False

    if result.get("skipped_reason") == "auto_chunking_required":
        logger.error("❌ FAIL: Large herd was still skipped due to 'auto_chunking_required'")
        return False

    # If we get here, the herd was processed (not skipped)
    logger.info("✅ SUCCESS: Large herd was processed instead of being skipped!")
    logger.info(f"📈 Result summary: {result.get('summary_stats', {})}")

    return True


def test_volume_management_integration():
    """Test that volume management system is properly configured for large herds."""

    sys.path.insert(0, "backend/pipelines/chr_pipeline")
    from datetime import date

    from bronze.volume_management import add_high_volume_herd, get_optimal_date_range, is_high_volume_herd

    logger.info("🔧 Testing volume management integration...")

    # Test adding a high-volume herd
    test_herd = 999999
    add_high_volume_herd(test_herd, max_days=30, volume_estimate=150000)

    # Check if it was added
    if not is_high_volume_herd(test_herd):
        logger.error("❌ FAIL: High volume herd was not properly registered")
        return False

    # Test date range chunking
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    date_ranges = get_optimal_date_range(test_herd, start_date, end_date)

    if len(date_ranges) <= 1:
        logger.error("❌ FAIL: Date range was not chunked for high-volume herd")
        return False

    logger.info(f"✅ SUCCESS: Date range chunked into {len(date_ranges)} chunks")
    logger.info(f"📅 First chunk: {date_ranges[0]}")
    logger.info(f"📅 Last chunk: {date_ranges[-1]}")

    return True


def main():
    """Run all tests."""
    logger.info("🚀 Starting CHR large herd fix verification tests...")

    tests = [
        ("Large Herd Processing", test_large_herd_processing),
        ("Volume Management Integration", test_volume_management_integration),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Running test: {test_name}")
        logger.info(f"{'=' * 50}")

        try:
            if test_func():
                logger.info(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
                failed += 1
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
            failed += 1

    logger.info(f"\n{'=' * 50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'=' * 50}")
    logger.info(f"✅ Passed: {passed}")
    logger.info(f"❌ Failed: {failed}")

    if failed == 0:
        logger.info("🎉 All tests passed! The fix appears to be working correctly.")
        return True
    else:
        logger.error("💥 Some tests failed. Please review the issues above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
