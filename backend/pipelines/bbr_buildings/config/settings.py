"""BBR Buildings Pipeline Configuration"""

import os
from pathlib import Path


class Settings:
    """Configuration settings for BBR Buildings Pipeline."""

    def __init__(self) -> None:
        # Data sources - use the original working URLs from the previous version
        self.inspire_bbr_url = os.getenv(
            "INSPIRE_BBR_URL",
            "https://ftp.dataforsyningen.dk/Bygninger_og_Adresser/Bygninger_og_Adresser_INSPIRE/DK_INSPIRE_BBR.zip",
        )

        # SDFE FTP base URL for dynamic URL parsing - restored from previous working version
        self.sdfe_ftp_base_url = os.getenv(
            "SDFE_FTP_BASE_URL",
            "https://ftp.sdfe.dk/main.html?download&weblink=2c950b3aadfeedc3b136df8525234819",
        )

        # GeoDanmark WFS URL for building cross-reference - restored from previous working version
        self.geodanmark_wfs_url = os.getenv(
            "GEODANMARK_WFS_URL",
            "https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS",
        )

        # Datafordeler credentials for GeoDanmark WFS
        self.datafordeler_username = os.getenv("DATAFORDELER_USERNAME")
        self.datafordeler_password = os.getenv("DATAFORDELER_PASSWORD")

        # GraphQL API settings
        self.datafordeler_graphql_api_key = os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
        self.graphql_endpoint = os.getenv(
            "GRAPHQL_ENDPOINT", "https://api.dataforsyningen.dk/graphql"
        )
        self.graphql_batch_size = self._get_int_env("GRAPHQL_BATCH_SIZE", 1000)
        self.graphql_max_retries = self._get_int_env("GRAPHQL_MAX_RETRIES", 3)

        # Google Cloud Storage
        self.gcs_bucket = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        self.gcs_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Output directories
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "data"))

        # Processing settings
        self.sample_size = self._get_int_env("SAMPLE_SIZE", None)
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # Spatial optimization settings (from field analysis learnings)
        self.spatial_chunk_size = self._get_int_env("SPATIAL_CHUNK_SIZE", 25000)
        self.memory_cleanup_frequency = self._get_int_env("MEMORY_CLEANUP_FREQUENCY", 5)
        self.min_building_area_m2 = self._get_float_env("MIN_BUILDING_AREA_M2", 5.0)
        self.min_geometry_area_m2 = self._get_float_env("MIN_GEOMETRY_AREA_M2", 1.0)
        self.duckdb_memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "12GB")

        # GitHub Actions optimization
        self.github_actions_mode = os.getenv("GITHUB_ACTIONS", "false").lower() == "true"
        self.enable_st_dump_optimization = (
            os.getenv("ENABLE_ST_DUMP_OPTIMIZATION", "true").lower() == "true"
        )

        # DuckDB Spatial v1.2.2 SPATIAL_JOIN operator settings
        self.enable_spatial_join_operator = (
            os.getenv("ENABLE_SPATIAL_JOIN_OPERATOR", "true").lower() == "true"
        )
        self.spatial_join_fallback_enabled = (
            os.getenv("SPATIAL_JOIN_FALLBACK_ENABLED", "true").lower() == "true"
        )

        # Spatial join performance settings
        self.spatial_join_build_side_memory_limit = os.getenv(
            "SPATIAL_JOIN_BUILD_SIDE_MEMORY_LIMIT", "8GB"
        )

        # Building category filters
        self.target_building_categories = ["residential", "agriculture", "education", "daycare"]

        # BBR usage codes of interest
        self.agricultural_bbr_codes = [
            210
        ]  # Bygning til landbrug, gartneri, råstofudvinding o.lign.
        self.residential_bbr_codes = [110, 120, 130, 140, 150, 160, 190, 510, 540]
        self.education_bbr_codes = [420, 421, 422, 429]  # Schools
        self.daycare_bbr_codes = [440, 441]  # Daycare centers

        # INSPIRE currentUse values
        self.residential_current_use = [
            "individualResidence",
            "collectiveResidence",
            "twoDwellings",
        ]
        self.agricultural_current_use = ["agriculture"]
        self.public_services_current_use = ["publicServices"]

    def _get_int_env(self, key: str, default: int | None) -> int | None:
        """Get integer environment variable."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def _get_float_env(self, key: str, default: float | None) -> float | None:
        """Get float environment variable."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    @property
    def has_datafordeler_credentials(self) -> bool:
        """Check if Datafordeler credentials are available."""
        return bool(self.datafordeler_username and self.datafordeler_password)

    @property
    def has_gcs_credentials(self) -> bool:
        """Check if GCS credentials are available."""
        return bool(self.gcs_credentials_path and Path(self.gcs_credentials_path).exists())

    @property
    def spatial_optimization_config(self) -> dict:
        """Get spatial optimization configuration."""
        return {
            "chunk_size": self.spatial_chunk_size,
            "memory_cleanup_frequency": self.memory_cleanup_frequency,
            "min_building_area_m2": self.min_building_area_m2,
            "min_geometry_area_m2": self.min_geometry_area_m2,
            "duckdb_memory_limit": self.duckdb_memory_limit,
            "enable_st_dump": self.enable_st_dump_optimization,
            "github_actions_mode": self.github_actions_mode,
            # DuckDB Spatial v1.2.2 SPATIAL_JOIN operator
            "enable_spatial_join_operator": self.enable_spatial_join_operator,
            "spatial_join_fallback_enabled": self.spatial_join_fallback_enabled,
            "spatial_join_build_side_memory_limit": self.spatial_join_build_side_memory_limit,
            "reference": "https://github.com/duckdb/duckdb-spatial/pull/545",
        }

    def get_bbr_codes_for_category(self, category: str) -> list:
        """Get BBR codes for a specific building category."""
        category_mapping = {
            "agricultural": self.agricultural_bbr_codes,
            "residential": self.residential_bbr_codes,
            "education": self.education_bbr_codes,
            "daycare": self.daycare_bbr_codes,
        }
        return category_mapping.get(category, [])

    def get_current_use_for_category(self, category: str) -> list:
        """Get INSPIRE currentUse values for a specific building category."""
        category_mapping = {
            "residential": self.residential_current_use,
            "agricultural": self.agricultural_current_use,
            "public_services": self.public_services_current_use,
        }
        return category_mapping.get(category, [])


def get_settings() -> Settings:
    """Get settings instance."""
    return Settings()
