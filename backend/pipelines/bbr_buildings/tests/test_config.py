"""Tests for BBR Buildings Pipeline configuration."""

from config import Settings, get_settings


def test_settings_creation():
    """Test that settings can be created with defaults."""
    settings = Settings()

    assert settings.max_workers == 4
    assert settings.chunk_size == 50000
    assert settings.environment == "dev"
    assert "agriculture" in settings.agricultural_current_use


def test_get_settings():
    """Test that get_settings function works."""
    settings = get_settings()

    assert isinstance(settings, Settings)
    assert settings.sdfe_ftp_base_url is not None
    assert settings.geodanmark_wfs_url is not None


def test_settings_with_env_vars(monkeypatch):
    """Test that settings respect environment variables."""
    monkeypatch.setenv("MAX_WORKERS", "8")
    monkeypatch.setenv("CHUNK_SIZE", "100000")
    monkeypatch.setenv("ENVIRONMENT", "production")

    settings = get_settings()

    assert settings.max_workers == 8
    assert settings.chunk_size == 100000
    assert settings.environment == "production"


def test_credentials_property():
    """Test credentials availability property."""
    settings = Settings()

    # Without credentials
    assert not settings.has_datafordeler_credentials

    # With credentials
    settings.datafordeler_username = "test_user"
    settings.datafordeler_password = "test_pass"
    assert settings.has_datafordeler_credentials


def test_cloud_storage_property():
    """Test cloud storage property."""
    settings = Settings()

    # Dev environment - no cloud storage
    assert not settings.use_cloud_storage

    # Production with bucket
    settings.environment = "production"
    settings.gcs_bucket = "test-bucket"
    assert settings.use_cloud_storage
