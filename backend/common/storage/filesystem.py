"""
R2/S3-compatible filesystem utilities using s3fs.

Provides cached filesystem access and DuckDB integration for optimal performance.
Uses Cloudflare R2 as primary storage backend, with GCS HMAC as fallback.
"""

import logging
import os
from functools import lru_cache

import duckdb
import s3fs

# Use standard logging - can be configured by calling code
logger = logging.getLogger("landbruget.storage")


@lru_cache(maxsize=1)
def get_r2_filesystem() -> s3fs.S3FileSystem:
    """
    Get cached s3fs filesystem instance for Cloudflare R2.

    Requires R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_ACCOUNT_ID
    environment variables to be set.

    Raises:
        EnvironmentError: If required R2 credentials are not set.
    """
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_account_id = os.getenv("R2_ACCOUNT_ID")

    if not (r2_access_key and r2_secret_key and r2_account_id):
        raise OSError(
            "R2 credentials not configured. Set R2_ACCESS_KEY_ID, "
            "R2_SECRET_ACCESS_KEY, and R2_ACCOUNT_ID environment variables."
        )

    endpoint_url = f"https://{r2_account_id}.r2.cloudflarestorage.com"
    logger.info("Using R2 S3-compatible authentication via s3fs")

    return s3fs.S3FileSystem(
        key=r2_access_key,
        secret=r2_secret_key,
        client_kwargs={"endpoint_url": endpoint_url},
        s3_additional_kwargs={"ACL": "private"},
    )


@lru_cache(maxsize=1)
def get_duckdb_with_r2() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection with R2 configured (native TYPE r2 secret)."""
    conn = duckdb.connect()

    # Try native cloud auth first (fastest), fallback to s3fs fsspec
    if setup_duckdb_cloud_auth(conn):
        logger.info("DuckDB configured with native cloud authentication")
    else:
        # Fallback to s3fs integration via fsspec
        try:
            r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
            r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
            r2_account_id = os.getenv("R2_ACCOUNT_ID")

            if r2_access_key and r2_secret_key and r2_account_id:
                endpoint_url = f"https://{r2_account_id}.r2.cloudflarestorage.com"
                fs = s3fs.S3FileSystem(
                    key=r2_access_key,
                    secret=r2_secret_key,
                    client_kwargs={"endpoint_url": endpoint_url},
                    s3_additional_kwargs={"ACL": "private"},
                )
                conn.register_filesystem(fs)
                logger.info("DuckDB configured with s3fs integration for R2")
            else:
                logger.warning("R2 credentials not found, DuckDB has no cloud storage access")
        except Exception as e:
            logger.warning(f"Failed to register s3fs with DuckDB: {e}")

    return conn


def setup_duckdb_cloud_auth(conn: duckdb.DuckDBPyConnection) -> bool:
    """Setup cloud storage authentication for a DuckDB connection.

    Tries R2 credentials first (TYPE r2), falls back to GCS HMAC (TYPE GCS).
    Installs and loads the httpfs extension as a prerequisite.

    Reference: https://duckdb.org/docs/stable/guides/network_cloud_storage/cloudflare_r2_import

    Returns:
        True if any cloud authentication was configured, False otherwise.
    """
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
    except Exception as e:
        logger.warning(f"Could not load httpfs extension: {e}")
        return False

    # Try R2 credentials first
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_account_id = os.getenv("R2_ACCOUNT_ID")

    if r2_access_key and r2_secret_key and r2_account_id:
        try:
            escaped_key = r2_access_key.replace("'", "''")
            escaped_secret = r2_secret_key.replace("'", "''")
            escaped_account = r2_account_id.replace("'", "''")
            conn.execute(f"""
                CREATE OR REPLACE SECRET r2_secret (
                    TYPE r2,
                    KEY_ID '{escaped_key}',
                    SECRET '{escaped_secret}',
                    ACCOUNT_ID '{escaped_account}'
                );
            """)
            logger.info("DuckDB R2 authentication configured")
            return True
        except Exception as e:
            logger.warning(f"Could not setup R2 authentication: {e}")

    # Fallback to GCS HMAC credentials
    storage_access_key = os.getenv("GCS_ACCESS_KEY_ID")
    gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

    if storage_access_key and gcs_secret_key:
        try:
            escaped_key = storage_access_key.replace("'", "''")
            escaped_secret = gcs_secret_key.replace("'", "''")
            conn.execute(f"""
                CREATE OR REPLACE SECRET gcs_hmac (
                    TYPE GCS,
                    KEY_ID '{escaped_key}',
                    SECRET '{escaped_secret}'
                );
            """)
            logger.info("DuckDB GCS HMAC authentication configured (legacy)")
            return True
        except Exception as e:
            logger.warning(f"Could not setup GCS authentication: {e}")

    logger.info("No R2 or GCS HMAC credentials found for DuckDB")
    return False


# Alias for backward compatibility with tests
_setup_native_r2_auth = setup_duckdb_cloud_auth
