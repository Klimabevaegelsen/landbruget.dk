"""Tests for arbejdstilsynet_inspections CLI interface.

Note: This pipeline requires playwright (browser binaries), so we test
via subprocess with the pipeline's own sys.path. If playwright is not
installed, the --help tests will fail with a clear error.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = PIPELINE_DIR.parent.parent


def _can_import(module_name: str) -> bool:
    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


def _run_cli(*args):
    """Run arbejdstilsynet main.py with given args."""
    env = {**os.environ, "PYTHONPATH": str(BACKEND_DIR)}
    return subprocess.run(
        [sys.executable, "main.py", *args],
        cwd=str(PIPELINE_DIR),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


@pytest.mark.skipif(not _can_import("playwright"), reason="playwright not installed")
class TestCLI:
    """Test the Click CLI interface."""

    def test_help_shows_expected_options(self):
        """--help should list all expected CLI options."""
        result = _run_cli("--help")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--stage" in result.stdout
        assert "--log-level" in result.stdout
        assert "--start-date" in result.stdout
        assert "--end-date" in result.stdout
        assert "--gcs-bucket" in result.stdout

    def test_invalid_stage_rejected(self):
        """An invalid --stage value should cause a non-zero exit."""
        result = _run_cli("--stage", "gold")
        assert result.returncode != 0

    def test_invalid_log_level_rejected(self):
        """An invalid --log-level value should cause a non-zero exit."""
        result = _run_cli("--log-level", "VERBOSE")
        assert result.returncode != 0
