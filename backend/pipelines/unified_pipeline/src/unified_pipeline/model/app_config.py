from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class GCSConfig(BaseSettings):
    credentials_path: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="GCS_",
    )
