#!/usr/bin/env python3
"""
Test the discovery system integration with main pipeline.
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def test_discovery_step_integration() -> bool:
    """Test that the discovery step is properly integrated in main.py"""

    logger.info("🧪 Testing discovery step integration...")

    try:
        # Add the path to sys.path
        sys.path.insert(0, "backend/pipelines/chr_pipeline")

        # Import main pipeline functions
        from main import get_required_steps, parse_arguments

        # Test that discovery step has correct dependencies
        discovery_deps = get_required_steps("herd_discovery")
        expected_deps = ["stamdata", "herds"]

        if discovery_deps != expected_deps:
            logger.error(
                f"❌ Discovery dependencies wrong: got {discovery_deps}, "
                f"expected {expected_deps}"
            )
            return False

        # Test that animal_movements depends on discovery
        animal_deps = get_required_steps("animal_movements")
        if "herd_discovery" not in animal_deps:
            logger.error(f"❌ Animal movements missing discovery dependency: {animal_deps}")
            return False

        logger.info("✅ Step dependencies correct")

        # Test argument parsing
        test_args = ["--steps", "herd_discovery", "--discovery-year", "2023", "--progress"]

        # Mock sys.argv for testing
        original_argv = sys.argv
        sys.argv = ["main.py"] + test_args

        try:
            args = parse_arguments()
            if args.get("discovery_year") != 2023:
                logger.error(
                    f"❌ Discovery year not parsed correctly: {args.get('discovery_year')}"
                )
                return False

            logger.info("✅ Argument parsing works")

        finally:
            sys.argv = original_argv

        logger.info("✅ Discovery step integration test passed!")
        return True

    except Exception as e:
        logger.error(f"❌ Discovery integration test failed: {e}")
        return False


def test_discovery_imports() -> bool:
    """Test that all discovery modules can be imported."""

    logger.info("🧪 Testing discovery module imports...")

    try:
        sys.path.insert(0, "backend/pipelines/chr_pipeline")

        # Test imports - verify modules can be imported
        import bronze.herd_discovery  # noqa: F401
        import bronze.volume_management  # noqa: F401

        logger.info("✅ All discovery modules imported successfully")
        return True

    except ImportError as e:
        logger.error(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False


def test_workflow_integration() -> bool:
    """Test that workflow files contain the discovery step."""

    logger.info("🧪 Testing GitHub Actions workflow integration...")

    try:
        # Check workflow file
        with open(".github/workflows/chr_pipeline.yml", "r") as f:
            workflow_content = f.read()

        required_elements = [
            "herd_discovery", "discovery_year:", "--discovery-year", 
            "Year for herd volume discovery"
        ]

        missing_elements = []
        for element in required_elements:
            if element not in workflow_content:
                missing_elements.append(element)

        if missing_elements:
            logger.error(f"❌ Missing workflow elements: {missing_elements}")
            return False

        logger.info("✅ Workflow integration correct")
        return True

    except Exception as e:
        logger.error(f"❌ Workflow integration test failed: {e}")
        return False


def main() -> bool:
    """Run all integration tests."""

    logger.info("🚀 Starting discovery system integration tests...")

    tests = [
        ("Discovery Step Integration", test_discovery_step_integration),
        ("Discovery Module Imports", test_discovery_imports),
        ("Workflow Integration", test_workflow_integration),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Test: {test_name}")
        logger.info("=" * 50)

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
    logger.info("INTEGRATION TEST SUMMARY")
    logger.info("=" * 50)
    logger.info(f"✅ Passed: {passed}")
    logger.info(f"❌ Failed: {failed}")

    if failed == 0:
        logger.info("\n🎉 All integration tests passed!")
        logger.info("The discovery system is properly integrated:")
        logger.info("- Pipeline dependencies are correct")
        logger.info("- Module imports work")
        logger.info("- GitHub Actions workflow is updated")
        return True
    else:
        logger.error(f"\n💥 {failed} tests failed. Please review the issues above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
