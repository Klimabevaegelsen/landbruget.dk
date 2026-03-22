"""Tests for DMA Scraper CLI interface."""

import os
import subprocess
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = PIPELINE_DIR.parent.parent


def _run_cli(*args):
    """Run DMA scraper main.py with given args."""
    env = {**os.environ, "PYTHONPATH": str(BACKEND_DIR)}
    return subprocess.run(
        [sys.executable, "main.py", *args],
        cwd=str(PIPELINE_DIR),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


class TestDMAScraperCLI:
    """Test DMA Scraper Click CLI interface."""

    def test_help(self):
        result = _run_cli("--help")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--stage" in result.stdout
        assert "bronze" in result.stdout
        assert "silver" in result.stdout
        assert "--log-level" in result.stdout
        assert "--total-pages" in result.stdout
        assert "--timestamp" in result.stdout
        assert "--start-date" in result.stdout
        assert "--end-date" in result.stdout

    def test_invalid_stage_rejected(self):
        result = _run_cli("--stage", "invalid")
        assert result.returncode != 0

    def test_invalid_log_level_rejected(self):
        result = _run_cli("--log-level", "TRACE")
        assert result.returncode != 0

    def test_invalid_total_pages_rejected(self):
        result = _run_cli("--total-pages", "not-a-number")
        assert result.returncode != 0
