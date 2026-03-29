"""Pytest configuration for unified_pipeline tests.

This conftest.py adds the backend directory to sys.path so that tests can
import from common.storage and other backend-level modules.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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


@pytest.fixture(autouse=True)
def _mock_r2_filesystem():
    """Auto-mock get_r2_filesystem to avoid requiring R2 credentials in tests."""
    from common.storage.filesystem import get_r2_filesystem

    get_r2_filesystem.cache_clear()
    mock_fs = MagicMock()
    # Ensure fs methods return proper types to avoid format string errors
    mock_fs.exists.return_value = True
    mock_fs.size.return_value = 0
    with (
        patch("common.storage.filesystem.get_r2_filesystem", return_value=mock_fs),
        patch("common.storage.core.get_r2_filesystem", return_value=mock_fs),
    ):
        yield
    get_r2_filesystem.cache_clear()
