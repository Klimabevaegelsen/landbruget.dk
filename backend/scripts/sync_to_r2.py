#!/usr/bin/env python3
"""
Sync GCS data to Cloudflare R2

This script syncs silver/ and gold/ directories from Google Cloud Storage
to Cloudflare R2 using rclone with R2's S3-compatible API.

Requirements:
    - rclone installed and available in PATH
    - Environment variables configured (see .env.example)

Usage:
    python sync_to_r2.py [--dry-run] [--verbose]

Environment Variables:
    GCS_BUCKET: Source GCS bucket name (e.g., 'landbruget-data')
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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add parent directory to path to access common modules
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from common.logging_utils import setup_pipeline_logger
except ImportError:
    # Fallback if common module not available
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    setup_pipeline_logger = None


class R2SyncError(Exception):
    """Custom exception for R2 sync errors."""

    pass


class GCSToR2Syncer:
    """Sync data from GCS to Cloudflare R2 using rclone."""

    def __init__(
        self,
        gcs_bucket: str,
        r2_bucket: str,
        r2_account_id: str,
        r2_access_key_id: str,
        r2_secret_access_key: str,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize syncer with credentials.

        Args:
            gcs_bucket: Source GCS bucket name
            r2_bucket: Destination R2 bucket name
            r2_account_id: Cloudflare R2 account ID
            r2_access_key_id: R2 access key ID
            r2_secret_access_key: R2 secret access key
            logger: Optional logger instance
        """
        self.gcs_bucket = gcs_bucket
        self.r2_bucket = r2_bucket
        self.r2_account_id = r2_account_id
        self.r2_access_key_id = r2_access_key_id
        self.r2_secret_access_key = r2_secret_access_key

        if logger:
            self.logger = logger
        elif setup_pipeline_logger:
            self.logger = setup_pipeline_logger(name="r2_sync", level="INFO", console_output=True)
        else:
            self.logger = logging.getLogger("r2_sync")

        self.sync_stats: Dict[str, Any] = {}

    def verify_rclone_installed(self) -> None:
        """Verify that rclone is installed and available."""
        try:
            result = subprocess.run(["rclone", "version"], capture_output=True, text=True, check=True)
            version = result.stdout.split("\n")[0]
            self.logger.info(f"Found {version}")
        except FileNotFoundError:
            raise R2SyncError(
                "rclone is not installed or not in PATH. "
                "Install with: brew install rclone (macOS) or apt install rclone (Linux)"
            )
        except subprocess.CalledProcessError as e:
            raise R2SyncError(f"Failed to verify rclone: {e}")

    def configure_rclone_remotes(self) -> None:
        """Configure rclone remotes for GCS and R2."""
        self.logger.info("Configuring rclone remotes...")

        # Configure GCS remote
        gcs_config = [
            "rclone",
            "config",
            "create",
            "gcs",
            "google cloud storage",
            "--non-interactive",
        ]

        # Add GCS authentication (uses application default credentials)
        gcs_env = os.environ.copy()
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            gcs_env["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            subprocess.run(
                gcs_config,
                env=gcs_env,
                capture_output=True,
                check=False,  # Don't fail if remote already exists
            )
            self.logger.info("GCS remote configured")
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"GCS remote configuration: {e.stderr.decode()}")

        # Configure R2 remote using S3 protocol
        r2_endpoint = f"https://{self.r2_account_id}.r2.cloudflarestorage.com"

        r2_config = [
            "rclone",
            "config",
            "create",
            "r2",
            "s3",
            f"access_key_id={self.r2_access_key_id}",
            f"secret_access_key={self.r2_secret_access_key}",
            f"endpoint={r2_endpoint}",
            "provider=Cloudflare",
            "--non-interactive",
        ]

        try:
            subprocess.run(
                r2_config,
                capture_output=True,
                check=False,  # Don't fail if remote already exists
            )
            self.logger.info("R2 remote configured")
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"R2 remote configuration: {e.stderr.decode()}")

    def sync_directory(self, directory: str, dry_run: bool = False, verbose: bool = False) -> Dict[str, Any]:
        """
        Sync a specific directory from GCS to R2.

        Args:
            directory: Directory name to sync (e.g., 'silver' or 'gold')
            dry_run: If True, show what would be synced without actually syncing
            verbose: If True, show detailed progress

        Returns:
            Dictionary with sync statistics
        """
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}Syncing {directory}/ directory...")

        source = f"gcs:{self.gcs_bucket}/{directory}"
        destination = f"r2:{self.r2_bucket}/{directory}"

        # Build rclone sync command
        cmd = [
            "rclone",
            "sync",
            source,
            destination,
            "--transfers",
            "8",  # Parallel transfers
            "--checkers",
            "8",  # Parallel file comparisons
            "--stats",
            "30s",  # Show stats every 30 seconds
            "--progress" if verbose else "--stats-one-line",
            "--exclude",
            "**/property_owners/**",  # Exclude property_owners
        ]

        if dry_run:
            cmd.append("--dry-run")

        if verbose:
            cmd.append("--verbose")

        start_time = datetime.now()

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            duration = (datetime.now() - start_time).total_seconds()

            stats = {
                "directory": directory,
                "source": source,
                "destination": destination,
                "duration_seconds": duration,
                "success": True,
                "dry_run": dry_run,
                "output": result.stdout,
            }

            self.logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}Successfully synced {directory}/ in {duration:.1f} seconds"
            )

            if verbose and result.stdout:
                self.logger.info(f"Output:\n{result.stdout}")

            return stats

        except subprocess.CalledProcessError as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = e.stderr if e.stderr else str(e)

            self.logger.error(f"Failed to sync {directory}/: {error_msg}")

            return {
                "directory": directory,
                "source": source,
                "destination": destination,
                "duration_seconds": duration,
                "success": False,
                "dry_run": dry_run,
                "error": error_msg,
            }

    def generate_manifest(self, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate manifest.json with metadata about synced datasets.

        Args:
            output_path: Optional path to save manifest file

        Returns:
            Manifest dictionary
        """
        self.logger.info("Generating manifest...")

        manifest = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source_bucket": f"gs://{self.gcs_bucket}",
            "destination_bucket": f"r2://{self.r2_bucket}",
            "sync_stats": self.sync_stats,
            "directories_synced": list(self.sync_stats.keys()),
            "total_duration_seconds": sum(stats.get("duration_seconds", 0) for stats in self.sync_stats.values()),
            "all_successful": all(stats.get("success", False) for stats in self.sync_stats.values()),
        }

        # Try to get additional metadata from R2
        try:
            manifest["datasets"] = self._get_dataset_metadata()
        except Exception as e:
            self.logger.warning(f"Could not fetch dataset metadata: {e}")
            manifest["datasets"] = {}

        # Save manifest if output path provided
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Manifest saved to {output_path}")

        return manifest

    def _get_dataset_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about datasets in R2.

        Returns:
            Dictionary with dataset metadata
        """
        datasets = {}

        for directory in ["silver", "gold"]:
            try:
                # Use rclone to list files and get sizes
                cmd = ["rclone", "size", f"r2:{self.r2_bucket}/{directory}", "--json"]

                result = subprocess.run(cmd, capture_output=True, text=True, check=True)

                size_info = json.loads(result.stdout)

                datasets[directory] = {
                    "total_files": size_info.get("count", 0),
                    "total_bytes": size_info.get("bytes", 0),
                    "total_size_mb": round(size_info.get("bytes", 0) / (1024 * 1024), 2),
                }

            except Exception as e:
                self.logger.warning(f"Could not get metadata for {directory}/: {e}")
                datasets[directory] = {"error": str(e)}

        return datasets

    def sync_all(self, dry_run: bool = False, verbose: bool = False) -> Dict[str, Any]:
        """
        Sync all required directories (silver and gold).

        Args:
            dry_run: If True, show what would be synced without actually syncing
            verbose: If True, show detailed progress

        Returns:
            Dictionary with overall sync statistics
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Starting GCS to R2 sync {'[DRY RUN]' if dry_run else ''}")
        self.logger.info(f"Source: gs://{self.gcs_bucket}")
        self.logger.info(f"Destination: r2://{self.r2_bucket}")
        self.logger.info("=" * 60)

        overall_start = datetime.now()

        # Sync silver and gold directories
        directories = ["silver", "gold"]

        for directory in directories:
            stats = self.sync_directory(directory=directory, dry_run=dry_run, verbose=verbose)
            self.sync_stats[directory] = stats

        overall_duration = (datetime.now() - overall_start).total_seconds()

        # Generate and display summary
        successful = sum(1 for stats in self.sync_stats.values() if stats.get("success"))
        total = len(self.sync_stats)

        self.logger.info("=" * 60)
        self.logger.info(f"Sync {'[DRY RUN] ' if dry_run else ''}completed in {overall_duration:.1f} seconds")
        self.logger.info(f"Successful: {successful}/{total} directories")

        if successful < total:
            self.logger.warning("Some directories failed to sync. Check logs above for details.")

        self.logger.info("=" * 60)

        return {
            "duration_seconds": overall_duration,
            "successful_syncs": successful,
            "total_syncs": total,
            "all_successful": successful == total,
            "details": self.sync_stats,
        }


def validate_environment() -> Dict[str, str]:
    """
    Validate that all required environment variables are set.

    Returns:
        Dictionary with environment variable values

    Raises:
        R2SyncError: If required environment variables are missing
    """
    required_vars = [
        "GCS_BUCKET",
        "R2_BUCKET",
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise R2SyncError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Please set them in your .env file or environment."
        )

    return {var: os.getenv(var) for var in required_vars}


def main() -> int:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Sync GCS data to Cloudflare R2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without actually syncing",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument(
        "--manifest",
        type=str,
        default="manifest.json",
        help="Path to save manifest file (default: manifest.json)",
    )

    args = parser.parse_args()

    # Setup logging
    if setup_pipeline_logger:
        logger = setup_pipeline_logger(
            name="r2_sync",
            level="DEBUG" if args.verbose else "INFO",
            console_output=True,
        )
    else:
        logger = logging.getLogger("r2_sync")
        logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    try:
        # Validate environment
        env_vars = validate_environment()

        # Create syncer
        syncer = GCSToR2Syncer(
            gcs_bucket=env_vars["GCS_BUCKET"],
            r2_bucket=env_vars["R2_BUCKET"],
            r2_account_id=env_vars["R2_ACCOUNT_ID"],
            r2_access_key_id=env_vars["R2_ACCESS_KEY_ID"],
            r2_secret_access_key=env_vars["R2_SECRET_ACCESS_KEY"],
            logger=logger,
        )

        # Verify rclone is installed
        syncer.verify_rclone_installed()

        # Configure rclone remotes
        syncer.configure_rclone_remotes()

        # Perform sync
        results = syncer.sync_all(dry_run=args.dry_run, verbose=args.verbose)

        # Generate manifest
        if not args.dry_run:
            manifest = syncer.generate_manifest(output_path=args.manifest)
            logger.info("\nManifest summary:")
            logger.info(f"  Total datasets: {len(manifest.get('datasets', {}))}")
            logger.info(f"  All successful: {manifest['all_successful']}")

        # Return appropriate exit code
        return 0 if results["all_successful"] else 1

    except R2SyncError as e:
        logger.error(f"Sync error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Sync interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
