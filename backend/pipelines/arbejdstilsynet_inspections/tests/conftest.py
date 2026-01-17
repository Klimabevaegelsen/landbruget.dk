"""
Shared pytest fixtures and configuration for arbejdstilsynet_inspections tests.
"""

import sys
from pathlib import Path

import pytest

# Add parent directory to Python path for imports
pipeline_dir = Path(__file__).resolve().parent.parent
if str(pipeline_dir) not in sys.path:
    sys.path.insert(0, str(pipeline_dir))


@pytest.fixture(autouse=True)
def suppress_logging():
    """Suppress logging during tests to reduce noise."""
    import logging

    logging.getLogger("SilverPipeline").setLevel(logging.CRITICAL)
    logging.getLogger("BronzePipeline").setLevel(logging.CRITICAL)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("CVR_USERNAME", "test_user")
    monkeypatch.setenv("CVR_PASSWORD", "test_password")


# Pytest configuration
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (requires full setup)"
    )
