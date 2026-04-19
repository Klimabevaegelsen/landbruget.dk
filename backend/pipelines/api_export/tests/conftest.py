"""Ensure api_export package directory is importable for test modules."""

import sys
from pathlib import Path

api_export_dir = Path(__file__).resolve().parent.parent
if str(api_export_dir) not in sys.path:
    sys.path.insert(0, str(api_export_dir))
