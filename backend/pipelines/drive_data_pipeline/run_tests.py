#!/usr/bin/env python3
"""
Script to run all test scripts for the Drive Data Pipeline.
"""

import subprocess
import sys
from pathlib import Path


def run_test(test_script):
    """Run a test script and return True if it succeeds."""
    print(f"\n{'-' * 80}")
    print(f"Running test: {test_script}")
    print(f"{'-' * 80}")
    
    try:
        subprocess.check_call([sys.executable, test_script])
        print(f"✓ {test_script} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {test_script} failed with exit code {e.returncode}")
        return False


def run_all_tests():
    """Run all test scripts."""
    # Get the directory of this script
    script_dir = Path(__file__).parent.absolute()
    
    # List of test scripts to run
    test_scripts = [
        script_dir / "standalone_tests" / "simple_test_duckdb.py",
        script_dir / "standalone_tests" / "simple_test_geopandas.py"
    ]
    
    # Run each test script
    results = []
    for script in test_scripts:
        success = run_test(script)
        results.append((script.name, success))
    
    # Print summary
    print("\n" + "=" * 80)
    print("Test Summary:")
    print("=" * 80)
    
    all_passed = True
    for script, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{status} - {script}")
        
        if not success:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("All tests passed!")
        return 0
    else:
        print("Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests()) 