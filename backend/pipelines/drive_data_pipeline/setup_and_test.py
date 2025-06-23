#!/usr/bin/env python3
"""
Script to install dependencies and run tests for the Drive Data Pipeline.
"""

import subprocess
import sys
from pathlib import Path


def main():
    """Install dependencies and run tests."""
    # Get the directory of this script
    script_dir = Path(__file__).parent.absolute()
    
    # Step 1: Install dependencies
    print("\n" + "=" * 80)
    print("Step 1: Installing dependencies")
    print("=" * 80)
    
    try:
        install_script = script_dir / "install_dependencies.py"
        subprocess.check_call([sys.executable, install_script])
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        return 1
    
    # Step 2: Run tests
    print("\n" + "=" * 80)
    print("Step 2: Running tests")
    print("=" * 80)
    
    try:
        test_script = script_dir / "run_tests.py"
        subprocess.check_call([sys.executable, test_script])
    except subprocess.CalledProcessError as e:
        print(f"Some tests failed: {e}")
        return 1
    
    print("\n" + "=" * 80)
    print("Setup and tests completed successfully!")
    print("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 