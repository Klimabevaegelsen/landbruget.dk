"""Backend-level pytest configuration.

This conftest.py ensures that the backend directory is in sys.path
before any test modules are imported, enabling imports like:
    from common.storage import StorageAccess

It also adds pipeline directories to sys.path to support pipeline-local imports.
"""

import sys
from pathlib import Path

# CRITICAL: Add paths at module import time, before pytest loads test files
backend_dir = Path(__file__).resolve().parent

# Add backend directory to sys.path immediately
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# Add chr_pipeline directory to support imports like: from silver.X import Y
chr_pipeline_dir = backend_dir / "pipelines" / "chr_pipeline"
if chr_pipeline_dir.exists() and str(chr_pipeline_dir) not in sys.path:
    sys.path.insert(0, str(chr_pipeline_dir))

# Add unified_pipeline src directory to support imports
unified_pipeline_src = backend_dir / "pipelines" / "unified_pipeline" / "src"
if unified_pipeline_src.exists() and str(unified_pipeline_src) not in sys.path:
    sys.path.insert(0, str(unified_pipeline_src))

# Add drive_data_pipeline directory
drive_pipeline_dir = backend_dir / "pipelines" / "drive_data_pipeline"
if drive_pipeline_dir.exists() and str(drive_pipeline_dir) not in sys.path:
    sys.path.insert(0, str(drive_pipeline_dir))


def pytest_configure(config):
    """
    Pytest configuration hook called before collecting tests.

    This is the earliest point to modify sys.path, ensuring pipeline modules
    can be imported when pytest discovers test files.
    """
    # Verify paths are set (they should be from module-level code above)
    paths_to_add = [
        backend_dir,
        chr_pipeline_dir,
        unified_pipeline_src,
        drive_pipeline_dir,
    ]

    for path in paths_to_add:
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
