"""
Tests for FVM WFS Silver layer.
"""

import pytest
from unified_pipeline.silver.fvm_wfs import FVMWFSSilver, FVMWFSSilverConfig


@pytest.fixture
def config() -> FVMWFSSilverConfig:
    """Create a test configuration."""
    return FVMWFSSilverConfig(save_local=True)


@pytest.fixture
def fvm_wfs_silver(config: FVMWFSSilverConfig) -> FVMWFSSilver:
    """Create a FVM WFS silver instance."""
    return FVMWFSSilver(config)


def test_fvm_wfs_silver_config() -> None:
    """Test FVM WFS silver configuration."""
    config = FVMWFSSilverConfig()

    assert config.name == "Danish FVM WFS Agricultural Data - Silver"
    assert config.type == "transformation"
    assert config.dataset_markblokke == "fvm_markblokke"
    assert config.dataset_marker == "fvm_marker"
    assert config.dataset_smaabiotoper == "fvm_smaabiotoper"
    assert config.dataset_organic_areas == "fvm_organic_areas"
    assert len(config.organic_areas_years) == 13  # 2012-2024
