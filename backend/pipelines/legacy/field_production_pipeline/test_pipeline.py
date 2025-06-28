#!/usr/bin/env python3
"""
Test script for the Field Production Pipeline

This script performs basic validation of the pipeline components to ensure
everything is properly configured and can be imported.
"""

import sys
from pathlib import Path

# Add paths for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent.parent))


def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")

    try:
        # Test main pipeline import
        from main import FieldProductionEstimator, parse_args, setup_logging

        print("✅ Main pipeline components imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import main pipeline components: {e}")
        return False

    try:
        # Test DST mapping import
        sys.path.append(str(Path(__file__).parent.parent / "dst_pipeline"))
        from dst_field_crop_mapping_table import get_dst_category

        print("✅ DST mapping imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import DST mapping: {e}")
        return False

    try:
        # Test storage interface import
        from common.storage_interface import GCSStorage

        print("✅ Storage interface imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import storage interface: {e}")
        return False

    return True


def test_argument_parsing():
    """Test command line argument parsing."""
    print("\nTesting argument parsing...")

    try:
        from main import parse_args

        # Test with minimal arguments
        sys.argv = ["test_pipeline.py", "--year", "2024"]
        args = parse_args()

        assert args.year == 2024
        assert not args.all_years
        assert args.output_dir == "data/silver"
        assert args.log_level == "INFO"

        print("✅ Argument parsing works correctly")
        return True

    except Exception as e:
        print(f"❌ Argument parsing failed: {e}")
        return False


def test_dst_mapping():
    """Test DST mapping functionality."""
    print("\nTesting DST mapping...")

    try:
        sys.path.append(str(Path(__file__).parent.parent / "dst_pipeline"))
        from dst_field_crop_mapping_table import get_dst_category

        # Test with a known crop
        result = get_dst_category("Vinterhvede")
        if result:
            print(f"✅ DST mapping works - Vinterhvede maps to {result}")
        else:
            print("⚠️  DST mapping returned None for Vinterhvede")

        # Test with unknown crop
        result = get_dst_category("NonexistentCrop")
        if result is None:
            print("✅ DST mapping correctly returns None for unknown crops")
        else:
            print(f"⚠️  DST mapping unexpectedly returned result for unknown crop: {result}")

        return True

    except Exception as e:
        print(f"❌ DST mapping test failed: {e}")
        return False


def test_estimator_initialization():
    """Test that the FieldProductionEstimator can be initialized."""
    print("\nTesting estimator initialization...")

    try:
        from main import FieldProductionEstimator

        # Test initialization without GCS
        estimator = FieldProductionEstimator(gcs_storage=None)

        print("✅ FieldProductionEstimator initialized successfully")
        print(f"   - DST data tables loaded: {len(estimator.dst_data)}")
        print(f"   - DST zone mapping available: {estimator.dst_zone_mapping is not None}")
        print(f"   - Spatial connection available: {estimator.spatial_conn is not None}")

        return True

    except Exception as e:
        print(f"❌ Estimator initialization failed: {e}")
        return False


def main():
    """Run all tests."""
    print("Field Production Pipeline Test Suite")
    print("=" * 50)

    tests = [
        test_imports,
        test_argument_parsing,
        test_dst_mapping,
        test_estimator_initialization,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")

    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Pipeline is ready to use.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
