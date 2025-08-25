"""Settings configuration for the Google Drive Data Pipeline."""

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator

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

    @model_validator(mode='after')
    def validate_storage_and_credentials(self) -> 'Settings':
        """Validate storage and credentials configuration."""
        # Validate GCS bucket
        if self.storage_type == StorageType.GCS and not self.gcs_bucket:
            raise ValueError("GCS bucket must be specified when using GCS storage")
        
        # Validate credentials file
        if not self.use_public_access and self.google_application_credentials is not None:
            # Check if it's an empty Path
            if str(self.google_application_credentials) == "":
                self.google_application_credentials = None
            elif not self.google_application_credentials.exists():
                raise ValueError(
                    f"Credentials file not found: {self.google_application_credentials}"
                )
        
        return self

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
