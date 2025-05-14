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
    
    # The requirements file path
    requirements_file = script_dir / "requirements.txt"
    
    # Check if requirements file exists
    if not requirements_file.exists():
        print(f"Requirements file not found at {requirements_file}")
        sys.exit(1)
    
    # Install the dependencies
    try:
        print(f"Installing packages from {requirements_file}...")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r",
            str(requirements_file)
        ])
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
            "tabula"
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