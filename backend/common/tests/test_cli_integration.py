"""Cross-pipeline CLI integration tests.

Snapshot tests that verify each pipeline's CLI contract (--help output,
accepted options) remains stable across refactoring.

These tests run each pipeline's main.py --help via subprocess to verify
the CLI interface without executing any pipeline logic.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
PIPELINES_DIR = BACKEND_DIR / "pipelines"


def _can_import(module_name: str) -> bool:
    """Check if a Python module can be imported."""
    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


def _make_env(extra_paths=None):
    """Build environment dict with PYTHONPATH including backend dir."""
    paths = [str(BACKEND_DIR)]
    if extra_paths:
        paths.extend(str(p) for p in extra_paths)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def _run_help(pipeline_dir: str, extra_paths=None) -> subprocess.CompletedProcess:
    """Run python main.py --help for a pipeline directory."""
    pipeline_path = PIPELINES_DIR / pipeline_dir
    main_py = pipeline_path / "main.py"
    if not main_py.exists():
        pytest.skip(f"{pipeline_dir}/main.py does not exist")

    return subprocess.run(
        [sys.executable, "main.py", "--help"],
        cwd=str(pipeline_path),
        capture_output=True,
        text=True,
        timeout=30,
        env=_make_env(extra_paths),
    )


class TestPipelineCLIHelp:
    """Verify --help works and expected options are present for each pipeline.

    These tests capture the current CLI contract so that Click migration
    can be verified to preserve the same interface.
    """

    def test_bmd_scraper_help(self):
        result = _run_help("bmd_scraper")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--stage" in result.stdout
        assert "bronze" in result.stdout
        assert "silver" in result.stdout
        assert "all" in result.stdout

    @pytest.mark.skipif(
        not _can_import("playwright"),
        reason="playwright not installed (requires browser binaries)",
    )
    def test_arbejdstilsynet_help(self):
        result = _run_help("arbejdstilsynet_inspections")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--stage" in result.stdout
        assert "--log-level" in result.stdout
        assert "--start-date" in result.stdout
        assert "--end-date" in result.stdout
        assert "--gcs-bucket" in result.stdout

    def test_svineflytning_help(self):
        result = _run_help("svineflytning_pipeline")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--stage" in result.stdout
        assert "--log-level" in result.stdout
        assert "--start-date" in result.stdout
        assert "--end-date" in result.stdout
        assert "--bronze-timestamp" in result.stdout
        assert "--progress" in result.stdout
        assert "--environment" in result.stdout
        assert "--test" in result.stdout
        assert "--max-concurrent-fetches" in result.stdout
        assert "--buffer-size" in result.stdout

    def test_climate_help(self):
        unified_src = PIPELINES_DIR / "unified_pipeline" / "src"
        result = _run_help("climate", extra_paths=[unified_src])
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--cvr" in result.stdout
        assert "--year" in result.stdout
        assert "--output" in result.stdout
        assert "--dry-run" in result.stdout
        assert "--log-level" in result.stdout

    def test_h3_pfas_help(self):
        result = _run_help("h3_pfas_exposure_pipeline")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--years" in result.stdout
        assert "--verbose" in result.stdout
        assert "--mode" in result.stdout

    def test_bbr_buildings_help(self):
        result = _run_help("bbr_buildings")
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "--layer" in result.stdout
        assert "bronze" in result.stdout
        assert "silver" in result.stdout
