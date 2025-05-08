"""
Application configuration module for unified pipeline settings.

This module defines configuration classes used throughout the application for
managing settings and environment variables. It uses Pydantic for validation
and automatic parsing of environment variables.
"""

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class GCSConfig(BaseSettings):
    """
    Google Cloud Storage configuration settings.

    This class manages GCS-related configuration including authentication
    credentials. It automatically loads values from environment variables
    with the prefix 'GCS_'.

    Attributes:
        credentials_path (Optional[str]): Path to the GCS service account
            credentials JSON file. If None, Application Default Credentials
            will be used.

    Example:
        >>> # Load from environment variables with GCS_ prefix
        >>> config = GCSConfig()
        >>> # Or set directly
        >>> config = GCSConfig(credentials_path="/path/to/credentials.json")
    """

    credentials_path: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="GCS_",
        extra="allow",
    )
