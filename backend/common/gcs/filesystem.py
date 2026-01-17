"""
GCS filesystem utilities using gcsfs.

Provides cached filesystem access and DuckDB integration for optimal performance.
"""

import logging
import os
from functools import lru_cache

import duckdb
import gcsfs

# Use standard logging - can be configured by calling code
logger = logging.getLogger("landbruget.gcs")


@lru_cache(maxsize=1)
def get_gcs_filesystem() -> gcsfs.GCSFileSystem:
    """
    Get cached gcsfs filesystem instance with optimal authentication.

    Priority order:
    1. HMAC credentials (fastest, no OAuth required)
    2. Service account authentication (fallback)
    """
    # Try HMAC authentication first (no OAuth required)
    gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
    gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

    if gcs_access_key and gcs_secret_key:
        logger.info("Using HMAC authentication for gcsfs (no OAuth required)")
        return gcsfs.GCSFileSystem(access_key_id=gcs_access_key, secret_access_key=gcs_secret_key)
    logger.info("Using service account authentication for gcsfs (requires OAuth)")
    return gcsfs.GCSFileSystem()


@lru_cache(maxsize=1)
def get_duckdb_with_gcs() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection with gcsfs registered (6x faster than HTTPFS)."""
    conn = duckdb.connect()

    # Try native HMAC first (fastest), fallback to gcsfs
    if _setup_native_gcs_auth(conn):
        logger.info("DuckDB configured with native GCS HMAC authentication")
    else:
        # Fallback to gcsfs integration
        try:
            from fsspec import filesystem

            # Use HMAC credentials if available for fsspec as well
            gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
            gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

            if gcs_access_key and gcs_secret_key:
                fs = filesystem("gs", access_key_id=gcs_access_key, secret_access_key=gcs_secret_key)
                logger.info("DuckDB configured with gcsfs integration using HMAC (no OAuth)")
            else:
                fs = filesystem("gs")
                logger.info("DuckDB configured with gcsfs integration using service account")

            conn.register_filesystem(fs)
        except Exception as e:
            logger.warning(f"Failed to register gcsfs with DuckDB: {e}")

    return conn


def _setup_native_gcs_auth(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Setup native GCS HMAC authentication if credentials are available.

    Uses DuckDB's httpfs extension with GCS secret type.
    Reference: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
    Reference: https://duckdb.org/docs/stable/guides/network_cloud_storage/gcs_import

    Returns:
        True if native authentication was configured successfully, False otherwise.
    """
    try:
        # Install and load httpfs extension
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Check for HMAC credentials
        gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
        gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

        if gcs_access_key and gcs_secret_key:
            # Create GCS secret for native access
            # Note: TYPE gcs automatically configures the correct GCS endpoint
            # No s3_region setting needed - that's only for S3
            # Use proper SQL escaping by doubling single quotes
            escaped_key = gcs_access_key.replace("'", "''")
            escaped_secret = gcs_secret_key.replace("'", "''")
            conn.execute(f"""
                CREATE OR REPLACE SECRET gcs_secret (
                    TYPE gcs,
                    KEY_ID '{escaped_key}',
                    SECRET '{escaped_secret}'
                );
            """)
            logger.info("Created DuckDB GCS secret with HMAC credentials")
            return True
        logger.info("GCS HMAC credentials not found, using gcsfs fallback")
        return False

    except Exception as e:
        logger.warning(f"Could not setup native GCS authentication: {e}")
        return False
