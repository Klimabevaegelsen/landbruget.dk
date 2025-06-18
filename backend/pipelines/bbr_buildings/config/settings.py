"""
Configuration settings for the BBR Buildings Pipeline.
"""

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
    """Configuration settings for the BBR Buildings Pipeline."""

    # Data Source Configuration
    sdfe_ftp_base_url: str = Field(
        default="https://ftp.sdfe.dk/main.html?download&weblink=2c950b3aadfeedc3b136df8525234819",
        description="SDFE FTP URL for DK_INSPIRE_BBR.zip",
    )
    geodanmark_wfs_url: str = Field(
        default="https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS",
        description="GeoDanmark WFS endpoint",
    )

    # Datafordeleren API credentials
    datafordeler_username: str | None = Field(None, description="Datafordeleren username")
    datafordeler_password: str | None = Field(None, description="Datafordeleren password")
    datafordeler_graphql_api_key: str | None = Field(
        None, description="Datafordeleren GraphQL API key for BBR queries"
    )

    # Storage Configuration
    storage_type: StorageType = Field(StorageType.LOCAL, description="Storage type (local or gcs)")
    gcs_bucket: str | None = Field(None, description="GCS bucket name (if using GCS)")
    environment: str = Field("dev", description="Environment (dev, prod)")

    # Processing Configuration
    max_workers: int = Field(4, description="Number of workers for parallel processing")
    chunk_size: int = Field(50000, description="Chunk size for data processing")

    # Logging settings
    log_level: LogLevel = Field(LogLevel.INFO, description="Logging level")

    # Data paths
    base_path: Path = Field(default_factory=lambda: Path("data"))
    bronze_path: Path = Field(default_factory=lambda: Path("data/bronze"))
    silver_path: Path = Field(default_factory=lambda: Path("data/silver"))

    # Building Usage Codes
    agricultural_usage_codes: tuple = Field(
        default=(210,), description="BBR agricultural usage codes"
    )
    residential_usage_codes: tuple = Field(
        default=(110, 120, 130, 140, 150, 160, 190, 510, 540),
        description="BBR residential usage codes",
    )
    educational_usage_codes: tuple = Field(
        default=(420, 421, 422, 429, 440, 441),
        description="BBR educational usage codes (includes both old and new codes: 420/421 schools, 422 university, 429 other education, 440/441 daycare)",
    )

    # INSPIRE Current Use Values
    agricultural_current_use: tuple = Field(
        default=("agriculture",), description="INSPIRE agricultural current use values"
    )
    residential_current_use: tuple = Field(
        default=("individualResidence", "collectiveResidence", "twoDwellings"),
        description="INSPIRE residential current use values",
    )
    public_services_current_use: tuple = Field(
        default=("publicServices",), description="INSPIRE public services current use values"
    )

    # Other construction types to include
    other_construction_current_use: tuple = Field(
        default=("agriculture", "industrial", "publicServices", "transport"),
        description="INSPIRE current use values for other constructions relevant to analysis",
    )

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

    @property
    def has_datafordeler_credentials(self) -> bool:
        """Check if Datafordeleren credentials are available."""
        return self.datafordeler_username is not None and self.datafordeler_password is not None

    @property
    def use_cloud_storage(self) -> bool:
        """Check if cloud storage should be used."""
        return self.gcs_bucket is not None and self.environment != "dev"

    def get_bronze_path_for_run(self, timestamp: str) -> Path:
        """Get the Bronze layer path for a specific run."""
        return self.bronze_path / timestamp

    def get_silver_path_for_run(self, timestamp: str) -> Path:
        """Get the Silver layer path for a specific run."""
        return self.silver_path / timestamp


def get_settings() -> Settings:
    """Get application settings."""
    # Detect environment and set appropriate storage defaults
    environment = os.getenv("ENVIRONMENT", "dev")

    # Auto-configure storage type based on environment
    if environment.lower() in ("production", "container"):
        default_storage_type = "gcs"
        default_gcs_bucket = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    else:
        default_storage_type = "local"
        default_gcs_bucket = None

    # Load values from environment variables
    return Settings(
        sdfe_ftp_base_url=os.getenv(
            "SDFE_FTP_BASE_URL",
            "https://ftp.sdfe.dk/main.html?download&weblink=2c950b3aadfeedc3b136df8525234819",
        ),
        geodanmark_wfs_url=os.getenv(
            "GEODANMARK_WFS_URL",
            "https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS",
        ),
        datafordeler_username=os.getenv("DATAFORDELER_USERNAME"),
        datafordeler_password=os.getenv("DATAFORDELER_PASSWORD"),
        datafordeler_graphql_api_key=os.getenv("DATAFORDELER_GRAPHQL_API_KEY"),
        storage_type=os.getenv("STORAGE_TYPE", default_storage_type),
        gcs_bucket=os.getenv("GCS_BUCKET", default_gcs_bucket),
        environment=environment,
        max_workers=int(os.getenv("MAX_WORKERS", "4")),
        chunk_size=int(os.getenv("CHUNK_SIZE", "50000")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
