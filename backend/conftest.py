"""Backend-level pytest configuration.

This conftest.py ensures that the backend directory is in sys.path
before any test modules are imported, enabling imports like:
    from common.gcs import GCSDataAccess
"""

import sys
from pathlib import Path

# Add backend directory to sys.path immediately
backend_dir = Path(__file__).resolve().parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))
