#!/usr/bin/env python3
"""
One-time migration: rename silver layer folders from space-separated to underscore-separated names.

Copies all files from old paths (e.g. "silver/animal welfare/") to new paths
(e.g. "silver/animal_welfare/"), then optionally deletes the old folders.

Usage:
    # Dry run (default) - shows what would be copied
    python backend/scripts/migrate_silver_folder_names.py

    # Execute the migration
    python backend/scripts/migrate_silver_folder_names.py --execute

    # Execute and delete old folders after copying
    python backend/scripts/migrate_silver_folder_names.py --execute --delete-old

R2 credentials are loaded from Google Secret Manager via common.secrets.init_secrets().
Set GCP_PROJECT (or GOOGLE_CLOUD_PROJECT) env var, or provide R2_* vars directly.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add backend root to path so we can import common.secrets (no pydantic dependency)
sys.path.insert(0, str(Path(__file__).parent.parent))

import importlib

import s3fs

# Import secrets directly to avoid common/__init__.py which pulls in pydantic
_secrets = importlib.import_module("common.secrets")
init_secrets = _secrets.init_secrets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _get_r2_filesystem() -> s3fs.S3FileSystem:
    """Create s3fs filesystem for R2, bootstrapping credentials from Secret Manager if needed."""
    # Bootstrap R2 credentials from Google Secret Manager (fills env vars if GCP_PROJECT is set)
    init_secrets()

    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_account_id = os.getenv("R2_ACCOUNT_ID")

    if not (r2_access_key and r2_secret_key and r2_account_id):
        raise OSError(
            "R2 credentials not configured. Either set GCP_PROJECT env var "
            "(to load from Secret Manager) or set R2_ACCESS_KEY_ID, "
            "R2_SECRET_ACCESS_KEY, and R2_ACCOUNT_ID directly."
        )

    return s3fs.S3FileSystem(
        key=r2_access_key,
        secret=r2_secret_key,
        client_kwargs={"endpoint_url": f"https://{r2_account_id}.r2.cloudflarestorage.com"},
    )


# Resolve bucket after secrets are loaded (deferred to migrate())
FOLDER_RENAMES = {
    "animal international movements": "animal_international_movements",
    "pig international movements": "pig_international_movements",
    "animal welfare": "animal_welfare",
    "animal mortality": "animal_mortality",
    "pig tail cutting": "pig_tail_cutting",
    "stable fires": "stable_fires",
    "slurry leaks": "slurry_leaks",
    "work permits": "work_permits",
    "transportation accidents": "transportation_accidents",
}


def migrate(execute: bool = False, delete_old: bool = False):
    fs = _get_r2_filesystem()
    bucket = os.getenv("STORAGE_BUCKET") or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET", "landbruget-data")

    for old_name, new_name in FOLDER_RENAMES.items():
        old_prefix = f"{bucket}/silver/{old_name}"
        new_prefix = f"{bucket}/silver/{new_name}"

        # List all files under the old prefix
        try:
            old_files = fs.glob(f"{old_prefix}/**")
        except Exception as e:
            logger.warning(f"Could not list {old_prefix}: {e}")
            continue

        # Filter to actual files (not directories)
        old_files = [f for f in old_files if not fs.isdir(f)]

        if not old_files:
            logger.info(f"No files found under '{old_name}' - skipping")
            continue

        logger.info(f"Found {len(old_files)} files under '{old_name}'")

        for old_path in old_files:
            # Build new path by replacing the folder name
            relative = old_path[len(old_prefix) :]
            new_path = f"{new_prefix}{relative}"

            if execute:
                # Check if destination already exists
                if fs.exists(new_path):
                    logger.info(f"  SKIP (exists): {new_path}")
                    continue

                logger.info(f"  COPY: {old_path} -> {new_path}")
                fs.copy(old_path, new_path)
            else:
                logger.info(f"  [DRY RUN] COPY: {old_path} -> {new_path}")

        if execute and delete_old:
            logger.info(f"  DELETE old folder: {old_prefix}/")
            try:
                fs.rm(old_prefix, recursive=True)
            except Exception as e:
                logger.warning(f"  Could not delete {old_prefix}: {e}")
        elif execute:
            logger.info(f"  Old folder kept: {old_prefix}/ (use --delete-old to remove)")

    logger.info("Migration complete." if execute else "Dry run complete. Use --execute to apply.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate silver folder names from spaces to underscores")
    parser.add_argument("--execute", action="store_true", help="Actually perform the migration (default: dry run)")
    parser.add_argument("--delete-old", action="store_true", help="Delete old folders after copying")
    args = parser.parse_args()

    if args.delete_old and not args.execute:
        parser.error("--delete-old requires --execute")

    migrate(execute=args.execute, delete_old=args.delete_old)
