"""
Application configuration module for unified pipeline settings.

This module defines configuration classes used throughout the application for
managing settings and environment variables. It uses Pydantic for validation
and automatic parsing of environment variables.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class GCSConfig(BaseSettings):
    """
    cloud storage configuration settings.

    This class manages cloud-storage-related configuration including authentication
    credentials. It automatically loads values from environment variables
    with the prefix 'GCS_' (kept for backward compatibility).

    Attributes:
        credentials_path (Optional[str]): Path to the cloud storage service account
            credentials JSON file. If None, Application Default Credentials
            will be used.

    Example:
        >>> # Load from environment variables with cloud storage_ prefix
        >>> config = GCSConfig()
        >>> # Or set directly
        >>> config = GCSConfig(credentials_path="/path/to/credentials.json")
    """

    credentials_path: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="GCS_",
        extra="allow",
    )
