"""Root pytest conftest.py that sets up PYTHONPATH before test collection.

This adds the backend directory to sys.path so tests can import from common.storage.
The path setup happens immediately when this module loads, before pytest collects tests.
"""

import sys
from pathlib import Path

# Calculate backend directory path
# This conftest is at: backend/pipelines/unified_pipeline/conftest.py
# Backend is at: backend/
_THIS_FILE = Path(__file__).resolve()
_BACKEND_DIR = _THIS_FILE.parent.parent.parent

# Add backend to sys.path IMMEDIATELY at module load time
# This must happen before pytest imports test modules
if _BACKEND_DIR.exists() and str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))


def pytest_configure(config):
    """Register custom markers and ensure backend path is in sys.path."""
    # Re-add in case it was removed
    if str(_BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(_BACKEND_DIR))

    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests requiring external resources",
    )
