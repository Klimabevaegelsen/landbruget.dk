"""Root-level pytest configuration for the landbruget.dk workspace.

This conftest.py ensures that all backend and pipeline directories are properly
added to sys.path before any test modules are imported.

CRITICAL: The module-level code runs when pytest loads this conftest.py, which
happens before test module collection begins.
"""

import sys
from pathlib import Path

# Get backend directory relative to workspace root
workspace_root = Path(__file__).resolve().parent
backend_dir = workspace_root / "backend"

# Add paths immediately at module import time - this executes before pytest
# starts collecting test modules
paths_to_add = [
    backend_dir,  # For: from common.gcs import GCSDataAccess
    backend_dir / "pipelines" / "chr_pipeline",  # For: from silver.X import Y
    backend_dir / "pipelines" / "unified_pipeline" / "src",  # For: from unified_pipeline.X import Y
    backend_dir / "pipelines" / "drive_data_pipeline",  # For: from drive_data_pipeline.X import Y
]

for path in paths_to_add:
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))
