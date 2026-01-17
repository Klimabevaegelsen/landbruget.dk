"""
Pytest configuration for h3_pfas_exposure_pipeline tests.

Sets up the Python path and common fixtures for all tests.
"""

import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
