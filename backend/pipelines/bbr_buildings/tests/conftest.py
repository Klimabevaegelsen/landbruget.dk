"""Pytest configuration for BBR buildings tests."""

import sys
from pathlib import Path

BBR_DIR = Path(__file__).resolve().parent.parent

# Top-level module names that several pipelines share (main.py, silver/, bronze/, ...).
# In a repo-wide pytest run a sibling pipeline may already own them in sys.modules.
_SHARED_TOP_LEVEL = ("main", "bronze", "silver", "config", "utils")


def _purge_sibling_pipeline_modules() -> None:
    for name in list(sys.modules):
        if name.split(".")[0] not in _SHARED_TOP_LEVEL:
            continue
        path = getattr(sys.modules[name], "__file__", None)
        if path is None or not Path(path).resolve().is_relative_to(BBR_DIR):
            del sys.modules[name]


# Add parent directory to path for imports
sys.path.insert(0, str(BBR_DIR))
_purge_sibling_pipeline_modules()
