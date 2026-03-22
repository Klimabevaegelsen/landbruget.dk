"""Tests for svineflytning_pipeline CLI interface."""

import os
import subprocess
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = PIPELINE_DIR.parent.parent


def _run_cli(*args):
    """Run svineflytning main.py with given args."""
    env = {**os.environ, "PYTHONPATH": str(BACKEND_DIR)}
    return subprocess.run(
        [sys.executable, "main.py", *args],
        cwd=str(PIPELINE_DIR),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


class TestHelpOutput:
    """Verify --help lists all expected options."""

    def test_help_shows_all_options(self):
        result = _run_cli("--help")
        assert result.returncode == 0, f"stderr: {result.stderr}"

        expected_options = [
            "--stage",
            "--start-date",
            "--end-date",
            "--bronze-timestamp",
            "--progress",
            "--environment",
            "--test",
            "--max-concurrent-fetches",
            "--buffer-size",
            "--log-level",
        ]
        for option in expected_options:
            assert option in result.stdout, f"Expected option {option} not found in help output"

    def test_help_shows_stage_choices(self):
        result = _run_cli("--help")
        assert "bronze" in result.stdout
        assert "silver" in result.stdout
        assert "all" in result.stdout

    def test_help_shows_environment_choices(self):
        result = _run_cli("--help")
        assert "prod" in result.stdout
        assert "test" in result.stdout

    def test_help_shows_log_level_choices(self):
        result = _run_cli("--help")
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            assert level in result.stdout


class TestInvalidStage:
    """Verify invalid stage values are rejected."""

    def test_invalid_stage_rejected(self):
        result = _run_cli("--stage", "gold")
        assert result.returncode != 0

    def test_invalid_stage_nonsense(self):
        result = _run_cli("--stage", "nonsense")
        assert result.returncode != 0
