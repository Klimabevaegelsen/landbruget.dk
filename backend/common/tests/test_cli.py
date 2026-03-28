"""Tests for shared CLI utilities.

Tests resolve_bucket(), PipelineRun, run_stages(), and Click decorators.
"""

import json

import click
import pytest
from click.testing import CliRunner
from common.cli import (
    PipelineRun,
    common_options,
    resolve_bucket,
    run_stages,
    stage_options,
)


class TestResolveBucket:
    """Test resolve_bucket() env var resolution."""

    def test_returns_override_when_provided(self, monkeypatch):
        monkeypatch.setenv("R2_BUCKET", "r2-bucket")
        monkeypatch.setenv("GCS_BUCKET", "gcs-bucket")
        assert resolve_bucket("my-override") == "my-override"

    def test_returns_r2_bucket_first(self, monkeypatch):
        monkeypatch.setenv("R2_BUCKET", "r2-bucket")
        monkeypatch.setenv("GCS_BUCKET", "gcs-bucket")
        assert resolve_bucket() == "r2-bucket"

    def test_falls_back_to_legacy_bucket_env(self, monkeypatch):
        monkeypatch.delenv("R2_BUCKET", raising=False)
        monkeypatch.setenv("GCS_BUCKET", "gcs-bucket")
        assert resolve_bucket() == "gcs-bucket"

    def test_returns_default_when_no_env(self, monkeypatch):
        monkeypatch.delenv("R2_BUCKET", raising=False)
        monkeypatch.delenv("GCS_BUCKET", raising=False)
        assert resolve_bucket() == "landbruget-data"


class TestPipelineRun:
    """Test PipelineRun metadata wrapper."""

    def test_init_starts_timer(self):
        run = PipelineRun("cadastral")
        assert run.start_time > 0
        assert run.source_key == "cadastral"

    def test_elapsed_returns_positive(self):
        run = PipelineRun("cadastral")
        assert run.elapsed >= 0

    def test_finish_creates_metadata_file(self, tmp_path):
        run = PipelineRun("cadastral")
        output = tmp_path / "cadastral_data.parquet"
        output.touch()

        result = run.finish(record_count=100, output_path=output)

        assert result is not None
        assert result.exists()
        content = json.loads(result.read_text())
        assert content["processing"]["record_count"] == 100

    def test_finish_creates_parent_dirs(self, tmp_path):
        run = PipelineRun("cadastral")
        output = tmp_path / "nested" / "dir" / "data.parquet"

        result = run.finish(output_path=output)

        assert result is not None
        assert result.exists()

    def test_finish_returns_none_without_output_path(self):
        run = PipelineRun("cadastral")
        result = run.finish(record_count=50)
        assert result is None

    def test_finish_with_source_datasets(self, tmp_path):
        run = PipelineRun("cadastral")
        output = tmp_path / "combined.parquet"
        output.touch()

        result = run.finish(
            source_datasets=["ds1", "ds2"],
            output_path=output,
        )

        content = json.loads(result.read_text())
        assert content["source_datasets"] == ["ds1", "ds2"]

    def test_logs_initialization(self, caplog):
        import logging

        logger = logging.getLogger("test_pipeline_run")
        logger.setLevel(logging.INFO)
        PipelineRun("cadastral", logger=logger)
        assert "metadata system initialized" in caplog.text


class TestRunStages:
    """Test run_stages() stage execution logic."""

    def test_runs_all_stages(self):
        calls = []
        results = run_stages(
            "all",
            {
                "bronze": lambda: calls.append("bronze") or "bronze_data",
                "silver": lambda: calls.append("silver") or "silver_data",
            },
        )
        assert calls == ["bronze", "silver"]
        assert results == {"bronze": "bronze_data", "silver": "silver_data"}

    def test_runs_single_stage(self):
        calls = []
        results = run_stages(
            "bronze",
            {
                "bronze": lambda: calls.append("bronze") or "data",
                "silver": lambda: calls.append("silver") or "data",
            },
        )
        assert calls == ["bronze"]
        assert "silver" not in results

    def test_skips_unselected_stages(self):
        calls = []
        run_stages(
            "silver",
            {
                "bronze": lambda: calls.append("bronze"),
                "silver": lambda: calls.append("silver") or "ok",
            },
        )
        assert calls == ["silver"]

    def test_stop_on_failure_exits_on_none(self):
        with pytest.raises(SystemExit):
            run_stages(
                "all",
                {
                    "bronze": lambda: None,
                    "silver": lambda: "ok",
                },
                stop_on_failure=True,
            )

    def test_stop_on_failure_exits_on_exception(self):
        def failing():
            raise ValueError("boom")

        with pytest.raises(SystemExit):
            run_stages(
                "bronze",
                {
                    "bronze": failing,
                },
                stop_on_failure=True,
            )

    def test_no_stop_on_failure_continues(self):
        results = run_stages(
            "all",
            {
                "bronze": lambda: None,
                "silver": lambda: "ok",
            },
            stop_on_failure=False,
        )
        assert results["bronze"] is None
        assert results["silver"] == "ok"

    def test_single_stage_failure_does_not_exit_when_not_all(self):
        """When running a single stage that returns None, don't exit."""
        results = run_stages(
            "bronze",
            {
                "bronze": lambda: None,
            },
            stop_on_failure=True,
        )
        assert results["bronze"] is None


class TestClickDecorators:
    """Test shared Click option decorators."""

    def test_common_options_adds_log_level(self):
        @click.command()
        @common_options
        def cli(log_level):
            click.echo(f"level={log_level}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--log-level", "DEBUG"])
        assert result.exit_code == 0
        assert "level=DEBUG" in result.output

    def test_common_options_default_info(self):
        @click.command()
        @common_options
        def cli(log_level):
            click.echo(f"level={log_level}")

        runner = CliRunner()
        result = runner.invoke(cli, [])
        assert "level=INFO" in result.output

    def test_stage_options_default_stages(self):
        @click.command()
        @stage_options()
        def cli(stage):
            click.echo(f"stage={stage}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--stage", "bronze"])
        assert result.exit_code == 0
        assert "stage=bronze" in result.output

    def test_stage_options_custom_stages(self):
        @click.command()
        @stage_options(stages=["fetch", "transform", "all"], default="fetch")
        def cli(stage):
            click.echo(f"stage={stage}")

        runner = CliRunner()
        result = runner.invoke(cli, [])
        assert "stage=fetch" in result.output

    def test_stage_options_rejects_invalid(self):
        @click.command()
        @stage_options()
        def cli(stage):
            click.echo(stage)

        runner = CliRunner()
        result = runner.invoke(cli, ["--stage", "invalid"])
        assert result.exit_code != 0

    def test_combined_decorators(self):
        @click.command()
        @stage_options()
        @common_options
        def cli(stage, log_level):
            click.echo(f"{stage}/{log_level}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--stage", "silver", "--log-level", "WARNING"])
        assert result.exit_code == 0
        assert "silver/WARNING" in result.output
