"""Settings configuration for the Google Drive Data Pipeline."""

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(env_path)


class StorageType(str, Enum):
    """Available storage types for the pipeline."""

    LOCAL = "local"
    GCS = "gcs"


class LogLevel(str, Enum):
    """Available log levels for the pipeline."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseModel):
    """Application settings loaded from environment variables."""

    # Google Drive settings
    google_drive_folder_id: str | None = Field(
        None, description="ID of the Google Drive folder to process"
    )
    google_application_credentials: Path | None = Field(
        None, description="Path to Google application credentials JSON file"
    )
    use_public_access: bool = Field(
        False, description="Use public access mode for Google Drive (no authentication required)"
    )

    # Storage settings
    storage_type: StorageType = Field(StorageType.LOCAL, description="Storage type (local or gcs)")
    gcs_bucket: str | None = Field(None, description="GCS bucket name (if using GCS)")

    # Logging settings
    log_level: LogLevel = Field(LogLevel.INFO, description="Logging level")

    # Processing settings
    max_workers: int = Field(4, description="Number of workers for parallel processing")

    # Data paths
    base_path: Path = Field(default_factory=lambda: Path("data"))
    bronze_path: Path = Field(default_factory=lambda: Path("data/bronze"))
    silver_path: Path = Field(default_factory=lambda: Path("data/silver"))

    class Config:
        """Pydantic model configuration."""

        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @validator("gcs_bucket")
    def validate_gcs_bucket(cls, v: str | None, values: dict) -> str | None:
        """Validate that GCS bucket is provided when using GCS storage."""
        if values.get("storage_type") == StorageType.GCS and not v:
            raise ValueError("GCS bucket must be specified when using GCS storage")
        return v

    @validator("google_application_credentials")
    def validate_credentials_file(cls, v: Path | None, values: dict) -> Path | None:
        """Validate that the credentials file exists if provided and not using public access."""
        use_public_access = values.get("use_public_access", False)

        # If using public access, credentials are optional
        if use_public_access:
            return v

        # If not using public access, credentials are required
        if v is not None:
            # Check if it's an empty Path
            if str(v) == "":
                return None

            # Check if file exists
            if not v.exists():
                raise ValueError(f"Credentials file not found: {v}")
        return v

    def get_bronze_path_for_run(self, timestamp: str) -> Path:
        """Get the Bronze layer path for a specific run."""
        return self.bronze_path / timestamp

    def get_silver_path_for_run(self, timestamp: str) -> Path:
        """Get the Silver layer path for a specific run."""
        return self.silver_path / timestamp


def get_settings() -> Settings:
    """Get application settings."""
    # Detect environment and set appropriate storage defaults
    environment = os.getenv("ENVIRONMENT", "development")

    # Auto-configure storage type based on environment
    if environment.lower() in ("production", "container"):
        default_storage_type = "gcs"
        default_gcs_bucket = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    else:
        default_storage_type = "local"
        default_gcs_bucket = None

    # Load values from environment variables
    return Settings(
        google_drive_folder_id=os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
        google_application_credentials=Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        else None,
        use_public_access=os.getenv("USE_PUBLIC_ACCESS", "false").lower() in ("true", "1", "yes"),
        storage_type=os.getenv("STORAGE_TYPE", default_storage_type),
        gcs_bucket=os.getenv("GCS_BUCKET", default_gcs_bucket),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_workers=int(os.getenv("MAX_WORKERS", "4")),
    )
