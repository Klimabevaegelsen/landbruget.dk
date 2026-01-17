"""Pytest configuration for unified_pipeline tests.

This conftest.py adds the backend directory to sys.path so that tests can
import from common.gcs and other backend-level modules.
"""

import sys
from pathlib import Path

import pytest

# Add backend directory to Python path for common module access
# Path structure: tests/conftest.py -> src/tests -> src -> unified_pipeline -> pipelines -> backend
# Or equivalently: 5 levels up from conftest.py gets us to backend
backend_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
if backend_dir.exists() and str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests requiring external resources"
    )
