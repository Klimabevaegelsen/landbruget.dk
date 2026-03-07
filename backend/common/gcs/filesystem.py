"""
R2/S3-compatible filesystem utilities using s3fs.

Provides cached filesystem access and DuckDB integration for optimal performance.
Migrated from gcsfs (GCS) to s3fs (Cloudflare R2) — all public APIs preserved.

Backward compatibility:
- get_gcs_filesystem() → alias for get_r2_filesystem()
- get_duckdb_with_gcs() → alias for get_duckdb_with_r2()
- _setup_native_gcs_auth() → alias for _setup_native_r2_auth()
"""

import logging
import os
from functools import lru_cache

import duckdb
import s3fs

# Use standard logging - can be configured by calling code
logger = logging.getLogger("landbruget.gcs")


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
        raise EnvironmentError(
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

    # Try native R2 auth first (fastest), fallback to s3fs fsspec
    if _setup_native_r2_auth(conn):
        logger.info("DuckDB configured with native R2 authentication")
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


def _setup_native_r2_auth(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Setup native R2 authentication using DuckDB's TYPE r2 secret.

    Uses DuckDB's httpfs extension with R2 secret type.
    Reference: https://duckdb.org/docs/stable/guides/network_cloud_storage/cloudflare_r2_import

    Returns:
        True if native authentication was configured successfully, False otherwise.
    """
    try:
        # Install and load httpfs extension
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Check for R2 credentials
        r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
        r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
        r2_account_id = os.getenv("R2_ACCOUNT_ID")

        if r2_access_key and r2_secret_key and r2_account_id:
            # Create R2 secret for native access
            # TYPE r2 automatically configures the correct R2 endpoint using ACCOUNT_ID
            # Use proper SQL escaping by doubling single quotes
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
            logger.info("Created DuckDB R2 secret with HMAC credentials")
            return True
        logger.info("R2 credentials not found, using s3fs fallback")
        return False

    except Exception as e:
        logger.warning(f"Could not setup native R2 authentication: {e}")
        return False


# Backward-compatible aliases
get_gcs_filesystem = get_r2_filesystem
get_duckdb_with_gcs = get_duckdb_with_r2
_setup_native_gcs_auth = _setup_native_r2_auth
