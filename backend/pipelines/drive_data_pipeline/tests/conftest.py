"""Common fixtures for tests."""

import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from drive_data_pipeline.config.settings import Settings
from drive_data_pipeline.utils.storage import LocalStorageManager


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_settings(temp_dir):
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
def storage_manager(temp_dir):
    """Create a local storage manager for testing."""
    return LocalStorageManager()


@pytest.fixture
def sample_file_content():
    """Create sample file content for testing."""
    return b"Sample file content for testing"


@pytest.fixture
def sample_json_content():
    """Create sample JSON content for testing."""
    return {
        "id": "test-id",
        "name": "test-file.pdf",
        "description": "Test file for unit tests"
    } 