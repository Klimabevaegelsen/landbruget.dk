"""Common fixtures for tests."""

import os
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest

# Add paths for imports - need both pipelines/ and backend/ for proper package resolution
_test_dir = Path(__file__).parent
_pipeline_root = _test_dir.parent  # drive_data_pipeline/
_pipelines_dir = _pipeline_root.parent  # pipelines/
_backend_root = _pipelines_dir.parent  # backend/

# Insert at beginning to take precedence
# Key insight: Add backend/ so that "common" package can be imported
# Add pipelines/ so that "drive_data_pipeline" package can be imported
sys.path.insert(0, str(_backend_root))  # For common package and other backend modules
sys.path.insert(0, str(_pipelines_dir))  # For drive_data_pipeline package

# Import common modules so they're available as top-level imports (as expected by code)
import common.pipeline_metadata

sys.modules["pipeline_metadata"] = common.pipeline_metadata

# Now we can import using package-style imports
from drive_data_pipeline.config.settings import Settings


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_settings(temp_dir: Path) -> mock.MagicMock:
    """Create mock settings for testing."""
    settings = mock.MagicMock(spec=Settings)
    settings.base_path = temp_dir
    settings.bronze_path = temp_dir / "bronze"
    settings.silver_path = temp_dir / "silver"
    settings.google_drive_folder_id = "test-folder-id"
    settings.storage_type.value = "local"
    settings.gcs_bucket = None

    # Create directories
    os.makedirs(settings.bronze_path, exist_ok=True)
    os.makedirs(settings.silver_path, exist_ok=True)

    return settings


@pytest.fixture
def storage_manager(temp_dir: Path) -> mock.MagicMock:
    """Create a mock storage manager for testing."""
    return mock.MagicMock()


@pytest.fixture
def sample_file_content() -> bytes:
    """Create sample file content for testing."""
    return b"Sample file content for testing"


@pytest.fixture
def sample_json_content() -> dict[str, str]:
    """Create sample JSON content for testing."""
    return {"id": "test-id", "name": "test-file.pdf", "description": "Test file for unit tests"}
