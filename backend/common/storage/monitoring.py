"""
Resource monitoring for GCS operations.

Tracks memory and disk usage during GCS operations,
with special handling for GitHub Actions environments.
"""

import os
import warnings

try:
    import psutil
except ImportError:
    psutil = None


class ResourceMonitor:
    """Monitor runner resource usage during GCS operations."""

    def __init__(self):
        self.max_memory_usage = 0
        self.max_disk_usage = 0

    def check_resources(self, operation_name: str) -> dict:
        """Check current resource usage with GitHub Actions compatibility."""
        # GITHUB ACTIONS FIX: Skip resource monitoring entirely in CI environment
        if os.getenv("GITHUB_ACTIONS") == "true":
            return {
                "memory_gb": 0.0,
                "disk_gb": 0.0,
                "memory_percent": 0.0,
                "disk_percent": 0.0,
            }

        # Normal resource monitoring for local/non-CI environments
        if psutil is None:
            return {
                "memory_gb": 0.0,
                "disk_gb": 0.0,
                "memory_percent": 0.0,
                "disk_percent": 0.0,
            }

        try:
            # Memory check
            memory = psutil.virtual_memory()
            memory_used_gb = (memory.total - memory.available) / (1024**3)

            # Disk check
            disk = psutil.disk_usage("/")
            disk_used_gb = (disk.total - disk.free) / (1024**3)

            # Track maximums
            self.max_memory_usage = max(self.max_memory_usage, memory_used_gb)
            self.max_disk_usage = max(self.max_disk_usage, disk_used_gb)

            # Alert if approaching limits (adjusted for development environment)
            if memory_used_gb > 20:  # 20 GB threshold for development
                warnings.warn(
                    f"{operation_name}: High memory usage {memory_used_gb:.1f} GB",
                    stacklevel=2,
                )

            if disk_used_gb > 400:  # 400 GB threshold for development
                warnings.warn(
                    f"{operation_name}: High disk usage {disk_used_gb:.1f} GB",
                    stacklevel=2,
                )

            return {
                "memory_gb": memory_used_gb,
                "disk_gb": disk_used_gb,
                "memory_percent": memory.percent,
                "disk_percent": (disk_used_gb / (disk.total / (1024**3))) * 100,
            }
        except Exception:
            return {
                "memory_gb": 0.0,
                "disk_gb": 0.0,
                "memory_percent": 0.0,
                "disk_percent": 0.0,
            }
