#!/usr/bin/env python3
"""
Sync only the latest snapshot of each dataset from GCS to Cloudflare R2.

Each dataset in GCS is stored as:
  silver/<dataset_name>/<YYYYMMDD_HHMMSS>/data.parquet

This script finds the latest snapshot for each dataset and copies it to R2 as:
  silver/<dataset_name>/data.parquet

This results in a much smaller transfer (one file per dataset instead of all
historical versions).

Usage:
    python sync_latest_to_r2.py [--dry-run] [--verbose]

Environment Variables:
    GCS_BUCKET: Source GCS bucket name
    R2_BUCKET: Destination R2 bucket name
    R2_ACCOUNT_ID: Cloudflare R2 account ID
    R2_ACCESS_KEY_ID: R2 access key ID
    R2_SECRET_ACCESS_KEY: R2 secret access key
"""

import json
import logging
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("latest_sync")

LAYERS = ["silver", "gold"]
EXCLUDE_DATASETS = {"property_owners"}


def rclone_lsd(path: str) -> list[str]:
    """List subdirectories of a remote path."""
    result = subprocess.run(["rclone", "lsd", path], capture_output=True, text=True, check=True)
    dirs = []
    for line in result.stdout.splitlines():
        parts = line.strip().split()
        if parts:
            dirs.append(parts[-1])
    return dirs


def get_latest_snapshot(remote_dataset_path: str) -> str | None:
    """Return the name of the latest snapshot folder (lexicographically largest YYYYMMDD_HHMMSS)."""
    try:
        snapshots = rclone_lsd(remote_dataset_path)
    except subprocess.CalledProcessError:
        return None
    # Filter to timestamp-like names and pick the largest (latest)
    ts_snapshots = [s for s in snapshots if len(s) == 15 and s[8] == "_"]
    if not ts_snapshots:
        return None
    return sorted(ts_snapshots)[-1]


def copy_file(src: str, dst: str, dry_run: bool = False) -> bool:
    """Copy a single file from src to dst using rclone copyto."""
    cmd = ["rclone", "copyto", src, dst, "--transfers", "4", "--s3-no-check-bucket"]
    if dry_run:
        cmd.append("--dry-run")
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to copy {src} -> {dst}: {e.stderr}")
        return False


def sync_latest(
    gcs_bucket: str,
    r2_bucket: str,
    r2_account_id: str,
    r2_access_key_id: str,
    r2_secret_access_key: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Find and copy the latest snapshot of every dataset."""

    # Configure rclone R2 remote
    r2_endpoint = f"https://{r2_account_id}.r2.cloudflarestorage.com"
    subprocess.run(
        [
            "rclone",
            "config",
            "create",
            "r2",
            "s3",
            f"access_key_id={r2_access_key_id}",
            f"secret_access_key={r2_secret_access_key}",
            f"endpoint={r2_endpoint}",
            "provider=Cloudflare",
            "--non-interactive",
        ],
        capture_output=True,
        check=False,
    )

    manifest_datasets = []
    total_copied = 0
    total_skipped = 0
    total_failed = 0

    for layer in LAYERS:
        gcs_layer = f"gcs:{gcs_bucket}/{layer}"
        r2_layer = f"r2:{r2_bucket}/{layer}"

        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Scanning {gcs_layer}...")

        try:
            datasets = rclone_lsd(gcs_layer)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list {gcs_layer}: {e.stderr}")
            continue

        for dataset in sorted(datasets):
            if dataset in EXCLUDE_DATASETS:
                logger.info(f"  Skipping excluded dataset: {dataset}")
                total_skipped += 1
                continue

            gcs_dataset = f"{gcs_layer}/{dataset}"
            latest = get_latest_snapshot(gcs_dataset)

            if not latest:
                # No timestamped snapshots — try copying data.parquet directly
                src = f"{gcs_dataset}/data.parquet"
                dst = f"{r2_layer}/{dataset}/data.parquet"
                logger.info(f"  {dataset}: no snapshots, trying direct copy")
            else:
                src = f"{gcs_dataset}/{latest}/data.parquet"
                dst = f"{r2_layer}/{dataset}/data.parquet"
                logger.info(f"  {dataset}: latest={latest}")

            if verbose:
                logger.info(f"    {src} -> {dst}")

            ok = copy_file(src, dst, dry_run=dry_run)
            if ok:
                total_copied += 1
                if not dry_run:
                    manifest_datasets.append(
                        {
                            "layer": layer,
                            "name": dataset,
                            "snapshot": latest,
                            "r2_path": f"{layer}/{dataset}/data.parquet",
                            "gcs_src": src,
                        }
                    )
            else:
                total_failed += 1

    logger.info("=" * 60)
    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Done: {total_copied} copied, {total_skipped} skipped, {total_failed} failed"
    )

    return {
        "copied": total_copied,
        "skipped": total_skipped,
        "failed": total_failed,
        "datasets": manifest_datasets,
    }


def build_manifest(
    r2_bucket: str,
    r2_base_url: str,
    datasets: list[dict],
    output_path: str = "manifest.json",
) -> None:
    """Generate manifest.json for the data explorer."""
    entries = []
    for ds in datasets:
        url = f"{r2_base_url.rstrip('/')}/{ds['layer']}/{ds['name']}/data.parquet"
        entries.append(
            {
                "name": ds["name"],
                "displayName": ds["name"].replace("_", " ").title(),
                "description": f"{ds['layer'].title()} layer dataset",
                "url": url,
                "layer": ds["layer"],
                "snapshot": ds.get("snapshot", ""),
                "rowCount": 0,
                "sizeBytes": 0,
                "columns": 0,
                "lastUpdated": datetime.now(UTC).isoformat(),
            }
        )

    manifest = {
        "version": "1.0",
        "generatedAt": datetime.now(UTC).isoformat(),
        "datasets": entries,
    }

    with Path(output_path).open("w") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest written to {output_path} ({len(entries)} datasets)")


def upload_manifest(r2_bucket: str, manifest_path: str, dry_run: bool = False) -> None:
    """Upload manifest.json to R2 root."""
    dst = f"r2:{r2_bucket}/manifest.json"
    cmd = ["rclone", "copyto", manifest_path, dst]
    if dry_run:
        cmd.append("--dry-run")
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        logger.info(f"Uploaded manifest to {dst}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to upload manifest: {e.stderr}")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Sync latest snapshots from GCS to R2")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--manifest", default="manifest.json")
    parser.add_argument(
        "--r2-base-url",
        default=os.getenv("R2_BASE_URL", "https://pub-b8c2f72ba51b4fe6804e9bb92280567c.r2.dev"),
    )
    args = parser.parse_args()

    required = [
        "GCS_BUCKET",
        "R2_BUCKET",
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        return 1

    results = sync_latest(
        gcs_bucket=os.environ["GCS_BUCKET"],
        r2_bucket=os.environ["R2_BUCKET"],
        r2_account_id=os.environ["R2_ACCOUNT_ID"],
        r2_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        r2_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    if not args.dry_run and results["datasets"]:
        build_manifest(
            r2_bucket=os.environ["R2_BUCKET"],
            r2_base_url=args.r2_base_url,
            datasets=results["datasets"],
            output_path=args.manifest,
        )
        upload_manifest(os.environ["R2_BUCKET"], args.manifest)

    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
