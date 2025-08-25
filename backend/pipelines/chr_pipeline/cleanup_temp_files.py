#!/usr/bin/env python3
"""
Cleanup utility for CHR pipeline temporary files.
This script can be run manually or as part of the pipeline to clean up
temporary files that might accumulate during processing.
"""

import glob
import logging
import os
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def cleanup_temp_files():
    """Clean up temporary files created by the CHR pipeline."""
    cleaned_files = 0
    total_size = 0

    # Define patterns for temporary files
    temp_patterns = [
        "/tmp/chr_streaming_*",
        "/tmp/temp_*",
        "/tmp/tmp*chr*",
        "/usr/data/silver/chr/*/temp_*",
        "/usr/data/silver/chr/*/_temp_*",
        "/usr/data/silver/chr/*/*.tmp",
        "/usr/data/silver/chr/*/*.jsonl",
    ]

    logger.info("Starting cleanup of CHR pipeline temporary files...")

    for pattern in temp_patterns:
        try:
            files = glob.glob(pattern)
            for file_path in files:
                try:
                    if os.path.exists(file_path):
                        # Get file size before deletion
                        file_size = os.path.getsize(file_path)

                        # Remove the file
                        os.unlink(file_path)

                        cleaned_files += 1
                        total_size += file_size
                        logger.info(f"Cleaned up: {file_path} ({file_size} bytes)")

                except Exception as e:
                    logger.warning(f"Could not remove {file_path}: {e}")

        except Exception as e:
            logger.warning(f"Error processing pattern {pattern}: {e}")

    # Clean up empty directories
    try:
        silver_base = Path("/usr/data/silver/chr")
        if silver_base.exists():
            for dir_path in silver_base.iterdir():
                if dir_path.is_dir():
                    try:
                        # Remove empty directories
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            logger.info(f"Removed empty directory: {dir_path}")
                    except Exception as e:
                        logger.debug(f"Could not remove directory {dir_path}: {e}")
    except Exception as e:
        logger.warning(f"Error cleaning up empty directories: {e}")

    # Log summary
    total_size_mb = total_size / (1024 * 1024)
    logger.info(f"Cleanup complete: {cleaned_files} files removed, {total_size_mb:.2f} MB freed")

    return cleaned_files, total_size


def monitor_disk_usage():
    """Monitor disk usage and log warnings if space is low."""
    try:
        import shutil

        # Check /tmp disk usage
        tmp_usage = shutil.disk_usage("/tmp")
        tmp_free_gb = tmp_usage.free / (1024 * 1024 * 1024)
        tmp_total_gb = tmp_usage.total / (1024 * 1024 * 1024)
        tmp_used_percent = (tmp_usage.used / tmp_usage.total) * 100

        logger.info(f"/tmp disk usage: {tmp_used_percent:.1f}% used, {tmp_free_gb:.1f}GB free of {tmp_total_gb:.1f}GB")

        if tmp_used_percent > 90:
            logger.warning(f"/tmp disk usage is high: {tmp_used_percent:.1f}%")

        # Check /usr/data disk usage if it exists
        if os.path.exists("/usr/data"):
            data_usage = shutil.disk_usage("/usr/data")
            data_free_gb = data_usage.free / (1024 * 1024 * 1024)
            data_total_gb = data_usage.total / (1024 * 1024 * 1024)
            data_used_percent = (data_usage.used / data_usage.total) * 100

            logger.info(
                f"/usr/data disk usage: {data_used_percent:.1f}% used, "
                f"{data_free_gb:.1f}GB free of {data_total_gb:.1f}GB"
            )

            if data_used_percent > 90:
                logger.warning(f"/usr/data disk usage is high: {data_used_percent:.1f}%")

    except Exception as e:
        logger.warning(f"Could not check disk usage: {e}")


def main():
    """Main function to run cleanup and monitoring."""
    logger.info("CHR Pipeline Cleanup Utility")

    # Monitor disk usage before cleanup
    monitor_disk_usage()

    # Perform cleanup
    cleaned_files, total_size = cleanup_temp_files()

    # Monitor disk usage after cleanup
    if cleaned_files > 0:
        logger.info("Disk usage after cleanup:")
        monitor_disk_usage()

    # Force garbage collection
    import gc

    gc.collect()

    logger.info("Cleanup utility finished")

    return 0 if cleaned_files >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
