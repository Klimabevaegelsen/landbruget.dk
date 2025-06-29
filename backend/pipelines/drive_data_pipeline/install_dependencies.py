#!/usr/bin/env python3
"""
Installation script for the Drive Data Pipeline dependencies.
This handles the complex dependencies like geopandas and its requirements.
"""

import subprocess
import sys
from pathlib import Path


def install_dependencies():
    """Install all required dependencies for the Drive Data Pipeline."""
    print("Installing Drive Data Pipeline dependencies...")

    # Get the directory of this script
    script_dir = Path(__file__).parent.absolute()

    # The pyproject.toml file path
    pyproject_file = script_dir / "pyproject.toml"

    # Check if pyproject.toml file exists
    if not pyproject_file.exists():
        print(f"pyproject.toml file not found at {pyproject_file}")
        sys.exit(1)

    # Install uv first if not available
    try:
        subprocess.check_call(
            [sys.executable, "-c", "import uv"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        print("Installing uv...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "uv"])

    # Install the dependencies using uv
    try:
        print(f"Installing packages from {pyproject_file}...")
        subprocess.check_call(
            [sys.executable, "-m", "uv", "pip", "install", "-e", "."], cwd=script_dir
        )
        print("Successfully installed dependencies.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        sys.exit(1)

    # Verify critical dependencies
    try:
        print("Verifying critical dependencies...")

        # Try importing some key packages
        import_checks = [
            "duckdb",
            "ibis",
            "geopandas",
            "pyarrow",
            "shapely",
            "pdf2image",
            "pytesseract",
            "pdfplumber",
            "tabula",
        ]

        for package in import_checks:
            try:
                __import__(package)
                print(f"✓ {package} installed successfully")
            except ImportError as e:
                print(f"✗ {package} import failed: {e}")
                raise

        print("All critical dependencies verified.")
    except Exception as e:
        print(f"Dependency verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    install_dependencies()
    print("\nSetup complete. You can now run the Drive Data Pipeline.")
