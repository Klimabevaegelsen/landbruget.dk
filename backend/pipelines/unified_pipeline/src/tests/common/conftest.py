"""Pytest configuration for common tests.

Adds backend to sys.path before test collection to enable common.storage imports.
"""

import sys
from pathlib import Path

# Add backend to path BEFORE pytest collects test modules
# Path: .../backend/pipelines/unified_pipeline/src/tests/common/conftest.py
backend_dir = Path(__file__).resolve().parents[5]

if backend_dir.exists() and str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))
