"""
Shared pytest fixtures and configuration for arbejdstilsynet_inspections tests.
"""

import sys
from pathlib import Path

import pytest

# Add parent directory to Python path for imports
pipeline_dir = Path(__file__).resolve().parent.parent


def _is_local_test(item) -> bool:
    return Path(str(item.path)).resolve().is_relative_to(pipeline_dir)


def _is_sibling_pipeline_module(module: object) -> bool:
    path = getattr(module, "__file__", None)
    return path is not None and not Path(path).resolve().is_relative_to(pipeline_dir)


def _prefer_pipeline_imports() -> None:
    """Put this pipeline first and clear cached sibling pipeline modules."""
    for module_name, module in list(sys.modules.items()):
        if (
            module_name == "silver"
            or module_name.startswith("silver.")
            or module_name == "bronze"
            or module_name.startswith("bronze.")
        ) and _is_sibling_pipeline_module(module):
            del sys.modules[module_name]
    if str(pipeline_dir) in sys.path:
        sys.path.remove(str(pipeline_dir))
    sys.path.insert(0, str(pipeline_dir))


_prefer_pipeline_imports()


def pytest_runtest_setup(item):
    """Keep top-level bronze/silver imports pointed at arbejdstilsynet tests."""
    if _is_local_test(item):
        _prefer_pipeline_imports()


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
