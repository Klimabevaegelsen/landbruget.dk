"""
Tests for FVM WFS Silver layer.
"""

from unittest.mock import MagicMock

import pytest

from unified_pipeline.silver.fvm_wfs import FVMWFSSilver, FVMWFSSilverConfig


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Create a mock GCS utility."""
    mock = MagicMock()
    mock.upload_blob = MagicMock()
    mock.download_blob = MagicMock()
    return mock


@pytest.fixture
def config() -> FVMWFSSilverConfig:
    """Create a test configuration."""
    return FVMWFSSilverConfig(save_local=True)


@pytest.fixture
def fvm_wfs_silver(config: FVMWFSSilverConfig, mock_gcs_util: MagicMock) -> FVMWFSSilver:
    """Create a FVM WFS silver instance."""
    return FVMWFSSilver(config, mock_gcs_util)


def test_fvm_wfs_silver_config() -> None:
    """Test FVM WFS silver configuration."""
    config = FVMWFSSilverConfig()

    assert config.name == "Danish FVM WFS Agricultural Data - Silver"
    assert config.type == "transformation"
    assert config.dataset_markblokke == "fvm_markblokke"
    assert config.dataset_marker == "fvm_marker"
    assert config.dataset_smaabiotoper == "fvm_smaabiotoper"
