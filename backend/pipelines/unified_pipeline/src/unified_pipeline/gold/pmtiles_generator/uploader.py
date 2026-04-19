"""PMTiles uploader for Cloudflare R2."""

import asyncio
import json
import os
import subprocess

from common.logging_utils import get_pipeline_logger

from .config import PMTilesGeneratorConfig

logger = get_pipeline_logger(__name__)


class CloudflareR2Uploader:
    """Uploads PMTiles to Cloudflare R2 using rclone."""

    def __init__(self, config: PMTilesGeneratorConfig):
        """Initialize the R2 uploader.

        Args:
            config: PMTiles generator configuration
        """
        self.config = config
        self.r2_remote = "r2"  # rclone remote name

    async def setup_rclone_config(self) -> bool:
        """Setup rclone configuration for Cloudflare R2.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if rclone is available
            result = subprocess.run(
                ["rclone", "version"], capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                logger.error("rclone not available")
                return False

            logger.info(
                f"rclone version: {result.stdout.strip().split()[1] if result.stdout else 'unknown'}"
            )

            # Check if R2 remote already exists (skip if write-only permissions)
            try:
                result = subprocess.run(
                    ["rclone", "listremotes"], capture_output=True, text=True, timeout=10
                )

                if result.returncode == 0 and f"{self.r2_remote}:" in result.stdout:
                    logger.info(f"rclone remote '{self.r2_remote}' already configured")
                    return True
            except Exception:
                # Ignore listremotes failures - might be write-only permissions
                logger.info(
                    "Cannot list remotes - assuming write-only permissions, will configure anyway"
                )

            # Configure R2 remote using environment variables
            access_key_id = os.environ.get("R2_ACCESS_KEY_ID")
            secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY")
            endpoint = os.environ.get("R2_ENDPOINT")

            if not all([access_key_id, secret_access_key, endpoint]):
                logger.error("Missing R2 credentials in environment variables")
                logger.error("Required: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT")
                return False

            # Create rclone config
            config_cmd = [
                "rclone",
                "config",
                "create",
                self.r2_remote,
                "s3",
                "provider",
                "Cloudflare",
                "access_key_id",
                access_key_id,
                "secret_access_key",
                secret_access_key,
                "endpoint",
                endpoint,
            ]

            result = subprocess.run(config_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                logger.info(f"Successfully configured rclone remote '{self.r2_remote}'")
                return True
            logger.error(f"Failed to configure rclone remote: {result.stderr}")
            return False

        except Exception as e:
            logger.error(f"Error setting up rclone config: {e}")
            return False

    async def upload_pmtiles(self, file_path: str, r2_key: str) -> str | None:
        """Upload a PMTiles file to Cloudflare R2.

        Args:
            file_path: Local path to PMTiles file
            r2_key: R2 object key (path in bucket)

        Returns:
            Public URL if successful, None otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"PMTiles file not found: {file_path}")
                return None

            # Ensure rclone is configured
            if not await self.setup_rclone_config():
                logger.error("Failed to setup rclone configuration")
                return None

            # Upload file with correct destination path
            r2_destination = f"{self.r2_remote}:{self.config.cloudflare_r2_bucket}/{r2_key}"

            logger.info(f"Uploading {file_path} to {r2_destination}")

            upload_cmd = [
                "rclone",
                "copyto",  # Use copyto for exact destination path
                file_path,
                r2_destination,
                "--progress",
                "--stats",
                "30s",
            ]

            # Run upload
            process = await asyncio.create_subprocess_exec(
                *upload_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                # Construct public URL
                public_url = f"{self.config.r2_base_url}/{r2_key}"

                # Get file size for logging
                size_mb = os.path.getsize(file_path) / (1024 * 1024)

                logger.info(
                    f"Successfully uploaded {os.path.basename(file_path)} ({size_mb:.1f} MB)"
                )
                logger.info(f"Public URL: {public_url}")

                return public_url
            logger.error(f"Upload failed with return code {process.returncode}")
            logger.error(f"stdout: {stdout.decode()}")
            logger.error(f"stderr: {stderr.decode()}")
            return None

        except Exception as e:
            logger.error(f"Error uploading PMTiles: {e}")
            return None

    async def upload_multiple_pmtiles(self, files: dict[str, str]) -> dict[str, str | None]:
        """Upload multiple PMTiles files to R2.

        Args:
            files: Dictionary mapping R2 keys to local file paths

        Returns:
            Dictionary mapping R2 keys to public URLs (None if failed)
        """
        logger.info(f"Uploading {len(files)} PMTiles files to R2")

        results = {}

        # Upload files sequentially to avoid overwhelming R2
        for r2_key, file_path in files.items():
            public_url = await self.upload_pmtiles(file_path, r2_key)
            results[r2_key] = public_url

            # Small delay between uploads
            await asyncio.sleep(1)

        # Log summary
        successful = sum(1 for url in results.values() if url is not None)
        logger.info(f"Successfully uploaded {successful}/{len(files)} PMTiles files")

        return results

    async def verify_upload(self, r2_key: str) -> bool:
        """Verify that a file exists in R2.

        Args:
            r2_key: R2 object key to verify

        Returns:
            True if file exists, False otherwise
        """
        try:
            # Use rclone to check if file exists
            check_cmd = [
                "rclone",
                "lsf",
                f"{self.r2_remote}:{self.config.cloudflare_r2_bucket}/{r2_key}",
            ]

            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)

            return result.returncode == 0 and r2_key.split("/")[-1] in result.stdout

        except Exception as e:
            logger.error(f"Error verifying upload: {e}")
            return False

    async def list_existing_pmtiles(self, prefix: str = "pmtiles/") -> list[str]:
        """List existing PMTiles files in R2.

        Args:
            prefix: Prefix to filter files

        Returns:
            List of R2 keys for existing PMTiles files
        """
        try:
            list_cmd = [
                "rclone",
                "lsf",
                f"{self.r2_remote}:{self.config.cloudflare_r2_bucket}/{prefix}",
                "--recursive",
            ]

            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                files = [
                    f"{prefix}{line.strip()}"
                    for line in result.stdout.strip().split("\n")
                    if line.strip()
                ]
                logger.info(f"Found {len(files)} existing PMTiles files in R2")
                return files
            logger.error(f"Failed to list R2 files: {result.stderr}")
            return []

        except Exception as e:
            logger.error(f"Error listing R2 files: {e}")
            return []

    async def delete_pmtiles(self, r2_key: str) -> bool:
        """Delete a PMTiles file from R2.

        Args:
            r2_key: R2 object key to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            delete_cmd = [
                "rclone",
                "delete",
                f"{self.r2_remote}:{self.config.cloudflare_r2_bucket}/{r2_key}",
            ]

            result = subprocess.run(delete_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                logger.info(f"Successfully deleted {r2_key}")
                return True
            logger.error(f"Failed to delete {r2_key}: {result.stderr}")
            return False

        except Exception as e:
            logger.error(f"Error deleting PMTiles: {e}")
            return False

    async def sync_pmtiles_directory(self, local_dir: str, r2_prefix: str = "pmtiles/") -> bool:
        """Sync a local directory of PMTiles to R2.

        Args:
            local_dir: Local directory containing PMTiles files
            r2_prefix: R2 prefix for uploaded files

        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(local_dir):
                logger.error(f"Local directory not found: {local_dir}")
                return False

            # Ensure rclone is configured
            if not await self.setup_rclone_config():
                logger.error("Failed to setup rclone configuration")
                return False

            # Sync directory
            r2_path = f"{self.r2_remote}:{self.config.cloudflare_r2_bucket}/{r2_prefix}"

            logger.info(f"Syncing {local_dir} to {r2_path}")

            sync_cmd = [
                "rclone",
                "sync",
                local_dir,
                r2_path,
                "--progress",
                "--stats",
                "30s",
                "--include",
                "*.pmtiles",  # Only sync PMTiles files
            ]

            # Run sync
            process = await asyncio.create_subprocess_exec(
                *sync_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info("Successfully synced PMTiles directory")
                return True
            logger.error(f"Sync failed with return code {process.returncode}")
            logger.error(f"stdout: {stdout.decode()}")
            logger.error(f"stderr: {stderr.decode()}")
            return False

        except Exception as e:
            logger.error(f"Error syncing PMTiles directory: {e}")
            return False

    def generate_upload_manifest(
        self, pmtiles_files: dict[str, str], year: int | None = None
    ) -> dict[str, str]:
        """Generate upload manifest mapping local files to R2 keys.

        Args:
            pmtiles_files: Dictionary mapping layer names to local file paths
            year: Optional year for year-specific files

        Returns:
            Dictionary mapping R2 keys to local file paths
        """
        manifest = {}

        for layer_name, file_path in pmtiles_files.items():
            if not file_path or not os.path.exists(file_path):
                continue

            # Generate R2 key based on layer type
            if layer_name in ["field_analysis"] and year:
                r2_key = f"pmtiles/{layer_name}_{year}.pmtiles"
            elif layer_name in [
                "bnbo_areas",
                "wetlands_all_2024",
                "water_projects_2024",
                "buildings_proximity_2024",
            ]:
                # Environmental layers with specific naming expected by frontend
                r2_key = f"pmtiles/{layer_name}.pmtiles"
            else:
                # Default naming
                r2_key = f"pmtiles/{layer_name}.pmtiles"

            manifest[r2_key] = file_path

        return manifest

    async def cleanup_old_pmtiles(
        self, current_files: dict[str, str], keep_versions: int = 3
    ) -> dict[str, bool]:
        """Clean up old PMTiles files, keeping only the most recent versions.

        Args:
            current_files: Dictionary mapping R2 keys to local file paths for current upload
            keep_versions: Number of versions to keep for each layer type

        Returns:
            Dictionary mapping deleted R2 keys to success status
        """
        logger.info(f"Cleaning up old PMTiles files (keeping {keep_versions} versions)")

        try:
            # Get all existing PMTiles files
            existing_files = await self.list_existing_pmtiles()

            if not existing_files:
                logger.info("No existing PMTiles files found for cleanup")
                return {}

            # Group files by layer type
            layer_files = {}
            for file_path in existing_files:
                # Extract layer name and year from file path
                filename = file_path.split("/")[-1].replace(".pmtiles", "")

                if filename.startswith("field_analysis_"):
                    # Year-specific files like field_analysis_2021.pmtiles
                    parts = filename.split("_")
                    if len(parts) >= 3 and parts[-1].isdigit():
                        layer_type = "_".join(parts[:-1])  # field_analysis
                        year = int(parts[-1])

                        if layer_type not in layer_files:
                            layer_files[layer_type] = []
                        layer_files[layer_type].append((year, file_path))
                else:
                    # Year-independent files like bnbo_areas.pmtiles
                    layer_type = filename
                    if layer_type not in layer_files:
                        layer_files[layer_type] = []
                    layer_files[layer_type].append((None, file_path))

            # Determine files to delete
            files_to_delete = []
            current_r2_keys = set(current_files.keys())

            for layer_type, files in layer_files.items():
                if layer_type == "field_analysis":
                    # For year-specific files, keep the most recent versions
                    files.sort(key=lambda x: x[0], reverse=True)  # Sort by year, newest first

                    # Skip files that are being uploaded in this run
                    files_not_current = [
                        (year, path) for year, path in files if path not in current_r2_keys
                    ]

                    # Delete old versions beyond keep_versions
                    if len(files_not_current) > keep_versions:
                        files_to_delete.extend(
                            [path for _, path in files_not_current[keep_versions:]]
                        )
                else:
                    # For year-independent files, keep only current version
                    # Delete old versions if not being uploaded in this run
                    for _, file_path in files:
                        if file_path not in current_r2_keys:
                            # This is an old version, mark for deletion
                            files_to_delete.append(file_path)

            # Delete old files
            deletion_results = {}
            for file_path in files_to_delete:
                logger.info(f"Deleting old PMTiles file: {file_path}")
                success = await self.delete_pmtiles(file_path)
                deletion_results[file_path] = success

            # Log summary
            successful_deletions = sum(1 for success in deletion_results.values() if success)
            logger.info(
                f"Cleanup completed: {successful_deletions}/{len(files_to_delete)} files deleted"
            )

            return deletion_results

        except Exception as e:
            logger.error(f"Error during PMTiles cleanup: {e}")
            return {}

    async def upload_with_cleanup(
        self, files: dict[str, str], cleanup_old: bool = True, keep_versions: int = 3
    ) -> dict[str, str | None]:
        """Upload PMTiles files and optionally clean up old versions.

        Args:
            files: Dictionary mapping R2 keys to local file paths
            cleanup_old: Whether to clean up old versions after successful upload
            keep_versions: Number of versions to keep for year-specific files

        Returns:
            Dictionary mapping R2 keys to public URLs (None if failed)
        """
        logger.info(f"Uploading {len(files)} PMTiles files with cleanup={cleanup_old}")

        # Upload new files
        upload_results = await self.upload_multiple_pmtiles(files)

        # Clean up old files if requested and uploads were successful
        if cleanup_old and any(url for url in upload_results.values()):
            successful_uploads = {
                r2_key: file_path
                for r2_key, file_path in files.items()
                if upload_results.get(r2_key) is not None
            }

            if successful_uploads:
                logger.info("Cleaning up old PMTiles versions")
                cleanup_results = await self.cleanup_old_pmtiles(successful_uploads, keep_versions)

                # Add cleanup info to results
                upload_results["_cleanup_results"] = cleanup_results

        return upload_results

    async def replace_pmtiles(self, r2_key: str, new_file_path: str) -> str | None:
        """Replace an existing PMTiles file with a new version.

        Args:
            r2_key: R2 object key to replace
            new_file_path: Local path to new PMTiles file

        Returns:
            Public URL if successful, None otherwise
        """
        try:
            logger.info(f"Replacing PMTiles file: {r2_key}")

            # Check if old file exists
            old_exists = await self.verify_upload(r2_key)

            # Upload new file
            public_url = await self.upload_pmtiles(new_file_path, r2_key)

            if public_url:
                if old_exists:
                    logger.info(f"Successfully replaced existing PMTiles: {r2_key}")
                else:
                    logger.info(f"Successfully uploaded new PMTiles: {r2_key}")
                return public_url
            logger.error(f"Failed to upload replacement PMTiles: {r2_key}")
            return None

        except Exception as e:
            logger.error(f"Error replacing PMTiles {r2_key}: {e}")
            return None

    async def create_deployment_summary(
        self,
        upload_results: dict[str, str | None],
        year: int | None = None,
        cleanup_results: dict[str, bool] | None = None,
    ) -> str:
        """Create a deployment summary with URLs and metadata.

        Args:
            upload_results: Dictionary mapping R2 keys to public URLs
            year: Optional year for the deployment
            cleanup_results: Optional cleanup results

        Returns:
            JSON string with deployment summary
        """
        summary = {
            "deployment_timestamp": asyncio.get_event_loop().time(),
            "year": year,
            "total_files": len(upload_results),
            "successful_uploads": sum(1 for url in upload_results.values() if url is not None),
            "failed_uploads": sum(1 for url in upload_results.values() if url is None),
            "files": {},
            "cleanup": {
                "performed": cleanup_results is not None,
                "deleted_files": len(cleanup_results) if cleanup_results else 0,
                "successful_deletions": sum(1 for success in cleanup_results.values() if success)
                if cleanup_results
                else 0,
            },
        }

        for r2_key, public_url in upload_results.items():
            if r2_key.startswith("_"):  # Skip internal keys like _cleanup_results
                continue

            file_info = {"public_url": public_url, "status": "success" if public_url else "failed"}

            # Add file size if URL is available
            if public_url:
                # Try to get file size from local file
                layer_name = r2_key.replace("pmtiles/", "").replace(".pmtiles", "")
                file_info["layer_name"] = layer_name

            summary["files"][r2_key] = file_info

        if cleanup_results:
            summary["cleanup"]["details"] = cleanup_results

        return json.dumps(summary, indent=2)
