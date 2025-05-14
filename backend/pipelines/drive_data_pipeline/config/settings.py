"""Settings configuration for the Google Drive Data Pipeline."""

import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv


# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
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
    google_drive_folder_id: Optional[str] = Field(None, description="ID of the Google Drive folder to process")
    google_application_credentials: Optional[Path] = Field(
        None, description="Path to Google application credentials JSON file"
    )

    # Storage settings
    storage_type: StorageType = Field(
        StorageType.LOCAL, description="Storage type (local or gcs)"
    )
    gcs_bucket: Optional[str] = Field(None, description="GCS bucket name (if using GCS)")

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
    def validate_gcs_bucket(cls, v: Optional[str], values: dict) -> Optional[str]:
        """Validate that GCS bucket is provided when using GCS storage."""
        if values.get("storage_type") == StorageType.GCS and not v:
            raise ValueError("GCS bucket must be specified when using GCS storage")
        return v

    @validator("google_application_credentials")
    def validate_credentials_file(cls, v: Optional[Path]) -> Optional[Path]:
        """Validate that the credentials file exists if provided."""
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
    # Load values from environment variables
    return Settings(
        google_drive_folder_id=os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
        google_application_credentials=Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")) if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") else None,
        storage_type=os.getenv("STORAGE_TYPE", "local"),
        gcs_bucket=os.getenv("GCS_BUCKET"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_workers=int(os.getenv("MAX_WORKERS", "4")),
    ) 