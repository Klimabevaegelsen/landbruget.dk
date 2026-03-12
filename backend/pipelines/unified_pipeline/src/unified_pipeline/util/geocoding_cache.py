"""
Geocoding Cache System
======================

A persistent caching system for geocoding results to avoid redundant API calls
in the CVR enrichment pipeline. Stores address->coordinates mappings in GCS
and loads them into DuckDB for fast lookups during pipeline execution.

Cache Strategy:
- DAWA ID Cache: adresse_id -> coordinates (primary)
- Address Text Cache: normalized address text -> coordinates (fallback)
- Persistent storage in GCS as Parquet files
- Fast in-memory lookups via DuckDB during pipeline execution
"""

import hashlib
import re
from datetime import datetime
from typing import Any

import duckdb
from common.gcs import GCSDataAccess

from unified_pipeline.util.log_util import Logger


class GeocodingCache:
    """
    Persistent geocoding cache to avoid redundant API calls.

    Provides two cache types:
    1. DAWA ID Cache: Direct lookup by adresse_id (most reliable)
    2. Address Text Cache: Normalized address string lookup (fallback)
    """

    def __init__(self, bucket: str = "landbruget-data", cache_version: int = 1):
        """
        Initialize the geocoding cache.

        Args:
            bucket: GCS bucket for cache storage
            cache_version: Cache version for invalidation purposes
        """
        self.log = Logger.get_logger()
        self.bucket = bucket
        self.cache_version = cache_version

        # Create DuckDB connection first
        self.conn = duckdb.connect()

        # Initialize GCS access with the same connection
        self.gcs_access = GCSDataAccess(connection=self.conn)

        # Cache paths in GCS
        self.dawa_cache_path = (
            f"gs://{bucket}/cache/geocoding/dawa_id_cache_v{cache_version}.parquet"
        )
        self.address_cache_path = (
            f"gs://{bucket}/cache/geocoding/address_text_cache_v{cache_version}.parquet"
        )

        # In-memory cache (DuckDB tables)
        self._initialize_cache_tables()
        self._load_existing_cache()

        self.log.info(f"Geocoding cache initialized (version {cache_version})")

    def _initialize_cache_tables(self) -> None:
        """Initialize DuckDB tables for cache storage."""

        # DAWA ID cache table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dawa_id_cache (
                adresse_id VARCHAR PRIMARY KEY,
                latitude DOUBLE,
                longitude DOUBLE,
                coordinate_system VARCHAR,
                srid INTEGER,
                geometry_wkt VARCHAR,
                geometry_geojson VARCHAR,
                coordinate_quality VARCHAR,
                coordinate_source VARCHAR,
                geocoded_timestamp TIMESTAMP,
                cache_version INTEGER
            )
        """)

        # Address text cache table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS address_text_cache (
                address_hash VARCHAR PRIMARY KEY,
                original_address VARCHAR,
                normalized_address VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                coordinate_system VARCHAR,
                srid INTEGER,
                geometry_wkt VARCHAR,
                geometry_geojson VARCHAR,
                coordinate_quality VARCHAR,
                coordinate_source VARCHAR,
                api_source VARCHAR,
                geocoded_timestamp TIMESTAMP,
                cache_version INTEGER
            )
        """)

    def _load_existing_cache(self) -> None:
        """Load existing cache data from GCS into DuckDB tables."""

        # Load DAWA ID cache
        try:
            if self.gcs_access.file_exists(self.dawa_cache_path):
                self.log.info("Loading existing DAWA ID cache from GCS")
                with self.gcs_access._temp_download(self.dawa_cache_path) as temp_file:
                    self.conn.execute(f"""
                        INSERT INTO dawa_id_cache
                        SELECT * FROM read_parquet('{temp_file}')
                    """)

                count = self.conn.execute("SELECT COUNT(*) FROM dawa_id_cache").fetchone()[0]
                self.log.info(f"Loaded {count:,} DAWA ID cache entries")
            else:
                self.log.info("No existing DAWA ID cache found")
        except Exception as e:
            self.log.warning(f"Failed to load DAWA ID cache: {e}")

        # Load address text cache
        try:
            if self.gcs_access.file_exists(self.address_cache_path):
                self.log.info("Loading existing address text cache from GCS")
                with self.gcs_access._temp_download(self.address_cache_path) as temp_file:
                    self.conn.execute(f"""
                        INSERT INTO address_text_cache
                        SELECT * FROM read_parquet('{temp_file}')
                    """)

                count = self.conn.execute("SELECT COUNT(*) FROM address_text_cache").fetchone()[0]
                self.log.info(f"Loaded {count:,} address text cache entries")
            else:
                self.log.info("No existing address text cache found")
        except Exception as e:
            self.log.warning(f"Failed to load address text cache: {e}")

    def _normalize_address(self, address: str) -> str:
        """
        Normalize address string for consistent caching.

        Args:
            address: Raw address string

        Returns:
            Normalized address string
        """
        if not address:
            return ""

        # Convert to lowercase and strip whitespace
        normalized = address.lower().strip()

        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", normalized)

        # Remove common punctuation
        normalized = re.sub(r"[,.]", "", normalized)

        # Normalize Danish characters
        return normalized.replace("æ", "ae").replace("ø", "oe").replace("å", "aa")

    def _create_address_hash(self, address: str) -> str:
        """
        Create a hash key for address text caching.

        Args:
            address: Address string

        Returns:
            SHA-256 hash of normalized address
        """
        normalized = self._normalize_address(address)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def lookup_by_dawa_id(self, adresse_id: str) -> dict[str, Any] | None:
        """
        Look up geocoding results by DAWA address ID.

        Args:
            adresse_id: DAWA address ID

        Returns:
            Cached geocoding result or None if not found
        """
        if not adresse_id:
            return None

        try:
            result = self.conn.execute(
                """
                SELECT
                    latitude, longitude, coordinate_system, srid,
                    geometry_wkt, geometry_geojson, coordinate_quality,
                    coordinate_source, geocoded_timestamp
                FROM dawa_id_cache
                WHERE adresse_id = ?
            """,
                [adresse_id],
            ).fetchone()

            if result:
                return {
                    "latitude": result[0],
                    "longitude": result[1],
                    "coordinate_system": result[2],
                    "srid": result[3],
                    "geometry_wkt": result[4],
                    "geometry_geojson": result[5],
                    "coordinate_quality": result[6],
                    "coordinate_source": result[7],
                    "dawa_fetch_timestamp": result[8].isoformat() if result[8] else None,
                    "cached": True,
                }

            return None

        except Exception as e:
            self.log.warning(f"Error looking up DAWA ID {adresse_id}: {e}")
            return None

    def lookup_by_address_text(self, address: str) -> dict[str, Any] | None:
        """
        Look up geocoding results by address text.

        Args:
            address: Full address string

        Returns:
            Cached geocoding result or None if not found
        """
        if not address:
            return None

        address_hash = self._create_address_hash(address)

        try:
            result = self.conn.execute(
                """
                SELECT
                    latitude, longitude, coordinate_system, srid,
                    geometry_wkt, geometry_geojson, coordinate_quality,
                    coordinate_source, api_source, geocoded_timestamp
                FROM address_text_cache
                WHERE address_hash = ?
            """,
                [address_hash],
            ).fetchone()

            if result:
                return {
                    "latitude": result[0],
                    "longitude": result[1],
                    "coordinate_system": result[2],
                    "srid": result[3],
                    "geometry_wkt": result[4],
                    "geometry_geojson": result[5],
                    "coordinate_quality": result[6],
                    "coordinate_source": result[7],
                    "api_source": result[8],
                    "dawa_fetch_timestamp": result[9].isoformat() if result[9] else None,
                    "cached": True,
                }

            return None

        except Exception as e:
            self.log.warning(f"Error looking up address text {address}: {e}")
            return None

    def store_dawa_result(self, adresse_id: str, geocoding_result: dict[str, Any]) -> None:
        """
        Store DAWA geocoding result in cache.

        Args:
            adresse_id: DAWA address ID
            geocoding_result: Geocoding result from DAWA API
        """
        if not adresse_id or not geocoding_result:
            return

        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO dawa_id_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    adresse_id,
                    geocoding_result.get("latitude"),
                    geocoding_result.get("longitude"),
                    geocoding_result.get("coordinate_system", "WGS84"),
                    geocoding_result.get("srid", 4326),
                    geocoding_result.get("geometry_wkt"),
                    geocoding_result.get("geometry_geojson"),
                    geocoding_result.get("coordinate_quality"),
                    geocoding_result.get("coordinate_source"),
                    datetime.now(),
                    self.cache_version,
                ],
            )

            self.log.debug(f"Cached DAWA result for address ID: {adresse_id}")

        except Exception as e:
            self.log.warning(f"Error storing DAWA result for {adresse_id}: {e}")

    def store_address_text_result(
        self, address: str, geocoding_result: dict[str, Any], api_source: str = "datavask"
    ) -> None:
        """
        Store address text geocoding result in cache.

        Args:
            address: Full address string
            geocoding_result: Geocoding result from API
            api_source: Source API ('dawa' or 'datavask')
        """
        if not address or not geocoding_result:
            return

        address_hash = self._create_address_hash(address)
        normalized_address = self._normalize_address(address)

        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO address_text_cache
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    address_hash,
                    address,
                    normalized_address,
                    geocoding_result.get("latitude"),
                    geocoding_result.get("longitude"),
                    geocoding_result.get("coordinate_system", "WGS84"),
                    geocoding_result.get("srid", 4326),
                    geocoding_result.get("geometry_wkt"),
                    geocoding_result.get("geometry_geojson"),
                    geocoding_result.get("coordinate_quality"),
                    geocoding_result.get("coordinate_source"),
                    api_source,
                    datetime.now(),
                    self.cache_version,
                ],
            )

            self.log.debug(f"Cached {api_source} result for address: {address[:50]}...")

        except Exception as e:
            self.log.warning(f"Error storing address text result for {address}: {e}")

    def get_cache_stats(self) -> dict[str, int]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        try:
            dawa_count = self.conn.execute("SELECT COUNT(*) FROM dawa_id_cache").fetchone()[0]
            address_count = self.conn.execute("SELECT COUNT(*) FROM address_text_cache").fetchone()[
                0
            ]

            return {
                "dawa_id_cache_entries": dawa_count,
                "address_text_cache_entries": address_count,
                "total_cache_entries": dawa_count + address_count,
            }
        except Exception as e:
            self.log.warning(f"Error getting cache stats: {e}")
            return {
                "dawa_id_cache_entries": 0,
                "address_text_cache_entries": 0,
                "total_cache_entries": 0,
            }

    def save_cache_to_gcs(self) -> None:
        """Save cache tables back to GCS for persistence."""

        try:
            # Save DAWA ID cache
            dawa_count = self.conn.execute("SELECT COUNT(*) FROM dawa_id_cache").fetchone()[0]
            if dawa_count > 0:
                self.log.info(f"Saving {dawa_count:,} DAWA ID cache entries to GCS")

                # Upload directly from DuckDB table to GCS
                self.gcs_access.upload_from_duckdb_table("dawa_id_cache", self.dawa_cache_path)
                self.log.info(f"DAWA ID cache saved to {self.dawa_cache_path}")

            # Save address text cache
            address_count = self.conn.execute("SELECT COUNT(*) FROM address_text_cache").fetchone()[
                0
            ]
            if address_count > 0:
                self.log.info(f"Saving {address_count:,} address text cache entries to GCS")

                # Upload directly from DuckDB table to GCS
                self.gcs_access.upload_from_duckdb_table(
                    "address_text_cache", self.address_cache_path
                )
                self.log.info(f"Address text cache saved to {self.address_cache_path}")

        except Exception as e:
            self.log.error(f"Error saving cache to GCS: {e}")
            raise

    def clear_cache(self) -> None:
        """Clear all cache data (for testing/debugging)."""
        try:
            self.conn.execute("DELETE FROM dawa_id_cache")
            self.conn.execute("DELETE FROM address_text_cache")
            self.log.info("Cache cleared")
        except Exception as e:
            self.log.warning(f"Error clearing cache: {e}")

    def cleanup(self) -> None:
        """Clean up resources and save cache to GCS."""
        try:
            self.save_cache_to_gcs()
            self.conn.close()
        except Exception as e:
            self.log.error(f"Error during cleanup: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - save cache to GCS."""
        self.cleanup()
