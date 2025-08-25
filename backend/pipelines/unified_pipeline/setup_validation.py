#!/usr/bin/env python3
"""
Unified Pipeline Setup Validation Script

This script validates that all dependencies, environment variables, and
configuration are properly set up for the unified pipeline.
"""

import importlib.util
import os
import sys
from pathlib import Path
from typing import List, Tuple


def check_python_version() -> Tuple[bool, str]:
    """Check if Python version meets requirements."""
    required_version = (3, 11)
    current_version = sys.version_info[:2]

    if current_version >= required_version:
        return True, f"✅ Python {sys.version.split()[0]} (meets requirement >= 3.11)"
    else:
        return False, f"❌ Python {sys.version.split()[0]} (requires >= 3.11)"


def check_required_packages() -> List[Tuple[bool, str]]:
    """Check if all required packages are installed."""
    required_packages = [
        "click",
        "pydantic",
        "python-dotenv",
        "loguru",
        "aiohttp",
        "gcsfs",
        "google.cloud.storage",
        "ibis",
        "duckdb",
        "pyarrow",
        "lxml",
        "zeep",
        "beautifulsoup4",
        "scikit-learn",
        "scipy",
        "tabulate",
        "tenacity",
        "tqdm",
        "xmltodict",
        "cryptography",
        "psutil",
        "simple_singleton",
    ]

    results = []
    for package in required_packages:
        try:
            if "." in package:
                # Handle packages with dots (like google.cloud.storage)
                module_name = package.split(".")[0]
                importlib.import_module(module_name)
            else:
                importlib.import_module(package.replace("-", "_"))
            results.append((True, f"✅ {package}"))
        except ImportError:
            results.append((False, f"❌ {package} (not installed)"))

    return results


def check_environment_variables() -> List[Tuple[bool, str]]:
    """Check if required environment variables are set."""
    required_env_vars = [
        ("GCS_BUCKET", "Google Cloud Storage bucket name"),
        ("GCS_ACCESS_KEY_ID", "GCS access key ID"),
        ("GCS_SECRET_ACCESS_KEY", "GCS secret access key"),
    ]

    optional_env_vars = [
        ("DATAFORDELER_USERNAME", "Datafordeler API username"),
        ("DATAFORDELER_PASSWORD", "Datafordeler API password"),
        ("CVR_USERID", "CVR Register API user ID"),
        ("CVR_PASSWORD", "CVR Register API password"),
        ("DMI_GOV_CLOUD_API_KEY", "DMI API key"),
        ("PLANTE_IT_USERNAME", "Plante IT username"),
        ("PLANTE_IT_PASSWORD", "Plante IT password"),
        ("LOG_LEVEL", "Logging level (INFO, DEBUG, WARNING, ERROR)"),
        ("ENVIRONMENT", "Environment (production, dev, local)"),
    ]

    results = []

    # Check required variables
    for var, description in required_env_vars:
        if os.getenv(var):
            results.append((True, f"✅ {var} ({description})"))
        else:
            results.append((False, f"❌ {var} ({description}) - REQUIRED"))

    # Check optional variables
    for var, description in optional_env_vars:
        if os.getenv(var):
            results.append((True, f"✅ {var} ({description})"))
        else:
            results.append((True, f"⚠️  {var} ({description}) - OPTIONAL"))

    return results


def check_unified_pipeline_imports() -> List[Tuple[bool, str]]:
    """Check if unified pipeline modules can be imported."""
    modules_to_check = [
        "unified_pipeline.app",
        "unified_pipeline.model.cli",
        "unified_pipeline.model.scheduling",
        "unified_pipeline.cli_scheduling",
        "unified_pipeline.util.log_util",
    ]

    results = []
    for module in modules_to_check:
        try:
            importlib.import_module(module)
            results.append((True, f"✅ {module}"))
        except ImportError as e:
            results.append((False, f"❌ {module} ({str(e)})"))

    return results


def check_scheduling_configuration() -> Tuple[bool, str]:
    """Check if scheduling configuration is valid."""
    try:
        from unified_pipeline.model.scheduling import PIPELINE_SCHEDULES, validate_dependencies

        errors = validate_dependencies()
        if not errors:
            pipeline_count = len(PIPELINE_SCHEDULES)
            return (
                True,
                f"✅ Scheduling configuration valid ({pipeline_count} pipelines configured)",
            )
        else:
            error_summary = "; ".join(errors[:3])  # Show first 3 errors
            if len(errors) > 3:
                error_summary += f" (and {len(errors) - 3} more)"
            return False, f"❌ Scheduling configuration errors: {error_summary}"

    except Exception as e:
        return False, f"❌ Failed to validate scheduling: {str(e)}"


def check_cli_functionality() -> Tuple[bool, str]:
    """Check if CLI commands work."""
    try:
        from click.testing import CliRunner

        from unified_pipeline.app import cli

        runner = CliRunner()

        # Test main help
        result = runner.invoke(cli, ["--help"])
        if result.exit_code != 0:
            return False, f"❌ Main CLI help failed: {result.output}"

        # Test scheduling help
        result = runner.invoke(cli, ["scheduling", "--help"])
        if result.exit_code != 0:
            return False, f"❌ Scheduling CLI help failed: {result.output}"

        # Test scheduling validation
        result = runner.invoke(cli, ["scheduling", "validate"])
        if result.exit_code != 0:
            return False, f"❌ Scheduling validation failed: {result.output}"

        return True, "✅ CLI functionality working"

    except Exception as e:
        return False, f"❌ CLI functionality check failed: {str(e)}"


def check_file_permissions() -> List[Tuple[bool, str]]:
    """Check file permissions and directory structure."""
    results = []

    # Check if we can create temp files
    try:
        import tempfile

        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            tmp.write(b"test")
        results.append((True, "✅ Temporary file creation"))
    except Exception as e:
        results.append((False, f"❌ Temporary file creation: {str(e)}"))

    # Check if source directory exists and is readable
    src_dir = Path(__file__).parent / "src" / "unified_pipeline"
    if src_dir.exists() and src_dir.is_dir():
        results.append((True, "✅ Source directory structure"))
    else:
        results.append((False, "❌ Source directory structure missing"))

    return results


def main():
    """Run all validation checks and report results."""
    print("🔍 Unified Pipeline Setup Validation")
    print("=" * 60)

    all_checks_passed = True

    # Python version check
    success, message = check_python_version()
    print("\n📍 Python Version:")
    print(f"  {message}")
    if not success:
        all_checks_passed = False

    # Package checks
    print("\n📦 Required Packages:")
    package_results = check_required_packages()
    for success, message in package_results:
        print(f"  {message}")
        if not success:
            all_checks_passed = False

    # Environment variable checks
    print("\n🌍 Environment Variables:")
    env_results = check_environment_variables()
    required_env_failed = False
    for success, message in env_results:
        print(f"  {message}")
        if not success and "REQUIRED" in message:
            all_checks_passed = False
            required_env_failed = True

    # Module import checks
    print("\n🔧 Module Imports:")
    import_results = check_unified_pipeline_imports()
    for success, message in import_results:
        print(f"  {message}")
        if not success:
            all_checks_passed = False

    # Scheduling configuration check
    print("\n📋 Scheduling Configuration:")
    success, message = check_scheduling_configuration()
    print(f"  {message}")
    if not success:
        all_checks_passed = False

    # CLI functionality check
    print("\n💻 CLI Functionality:")
    success, message = check_cli_functionality()
    print(f"  {message}")
    if not success:
        all_checks_passed = False

    # File permissions check
    print("\n📁 File System:")
    file_results = check_file_permissions()
    for success, message in file_results:
        print(f"  {message}")
        if not success:
            all_checks_passed = False

    # Final summary
    print("\n" + "=" * 60)
    if all_checks_passed:
        print("🎉 ALL CHECKS PASSED! Your unified pipeline setup is ready.")
        print("\n💡 Quick start commands:")
        print("  python -m unified_pipeline scheduling list-schedules")
        print("  python -m unified_pipeline scheduling execution-order --frequency monthly")
        print("  python -m unified_pipeline run -s dst -j bronze")
        return 0
    else:
        print("❌ SOME CHECKS FAILED! Please fix the issues above.")

        if required_env_failed:
            print("\n🔧 To fix environment variable issues:")
            print("  1. Create a .env file in the unified_pipeline directory")
            print("  2. Add the required environment variables")
            print("  3. Source the .env file or restart your shell")

        print("\n📦 To install missing packages:")
        print("  uv pip install -e .")

        return 1


if __name__ == "__main__":
    sys.exit(main())
