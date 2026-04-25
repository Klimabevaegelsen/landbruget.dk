#!/usr/bin/env python3
"""dyrenesdetektiv_scraper pipeline entry point.

Bronze: paginate the WP REST `kontrol` index, snapshot the `kontrol_tag`
taxonomy, download every detail HTML page (throttled).
Silver: parse all bronze HTML into a single Parquet table.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import click
from common.cli import PipelineRun, common_options, stage_options
from common.logging_utils import setup_pipeline_logger
from dotenv import find_dotenv, load_dotenv

# Ensure pipeline-local imports resolve when invoked from anywhere.
PIPELINE_DIR = Path(__file__).resolve().parent
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from bronze import DyrenesDetektivBronze  # noqa: E402
from silver import run_silver  # noqa: E402

load_dotenv(find_dotenv(usecwd=True))

logger = setup_pipeline_logger("dyrenesdetektiv_scraper", level=os.getenv("LOG_LEVEL", "INFO"))

DEFAULT_BRONZE_DIR = Path(os.getenv("BRONZE_OUTPUT_DIR", "data/bronze"))
DEFAULT_SILVER_DIR = Path(os.getenv("SILVER_OUTPUT_DIR", "data/silver"))


def _resolve_latest_run(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    runs = [p for p in parent.iterdir() if p.is_dir()]
    return max(runs, key=lambda p: p.name) if runs else None


def run_bronze(bronze_dir: Path, timestamp: str, limit: int | None) -> Path:
    output = bronze_dir / timestamp
    runner = DyrenesDetektivBronze(output_dir=output)
    manifest = runner.run(limit=limit)
    logger.info(
        "Bronze: %s details (%s errors), %.1fs",
        manifest["detail_count"],
        manifest["detail_errors"],
        manifest["duration_seconds"],
    )
    return output


def run_silver_stage(bronze_run_dir: Path, silver_dir: Path, timestamp: str) -> tuple[Path, dict]:
    output = silver_dir / timestamp
    output.mkdir(parents=True, exist_ok=True)
    summary = run_silver(bronze_run_dir, output)
    logger.info(
        "Silver: %s records, %s with CHR, %s with CVR (%s bytes)",
        summary["record_count"],
        summary["valid_chr_count"],
        summary["valid_cvr_count"],
        summary["parquet_size_bytes"],
    )
    return output, summary


@click.command()
@stage_options(["bronze", "silver", "all"])
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Cap on detail-page fetches in bronze stage (smoke testing).",
)
@click.option(
    "--bronze-timestamp",
    type=str,
    default=None,
    help="Override bronze run timestamp to read for silver stage (default: latest).",
)
@common_options
def main(stage: str, limit: int | None, bronze_timestamp: str | None, log_level: str) -> None:
    """Entry point for the dyrenesdetektiv_scraper pipeline."""
    global logger
    logger = setup_pipeline_logger("dyrenesdetektiv_scraper", level=log_level)
    logger.info("Starting dyrenesdetektiv_scraper (stage=%s, limit=%s)", stage, limit)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pipeline_run = PipelineRun("dyrenesdetektiv_kontrol", logger=logger)

    bronze_run_dir: Path | None = None
    silver_run_dir: Path | None = None
    silver_summary: dict | None = None

    if stage in ("bronze", "all"):
        bronze_run_dir = run_bronze(DEFAULT_BRONZE_DIR, timestamp, limit)

    if stage in ("silver", "all"):
        if bronze_run_dir is None:
            if bronze_timestamp:
                bronze_run_dir = DEFAULT_BRONZE_DIR / bronze_timestamp
            else:
                bronze_run_dir = _resolve_latest_run(DEFAULT_BRONZE_DIR)
            if bronze_run_dir is None or not bronze_run_dir.exists():
                logger.error("No bronze run found at %s", DEFAULT_BRONZE_DIR)
                sys.exit(1)
            logger.info("Silver reading bronze run: %s", bronze_run_dir)
        silver_run_dir, silver_summary = run_silver_stage(
            bronze_run_dir, DEFAULT_SILVER_DIR, bronze_run_dir.name
        )

    if silver_summary:
        try:
            pipeline_run.finish(
                record_count=silver_summary["record_count"],
                file_size_bytes=silver_summary["parquet_size_bytes"],
                output_path=silver_run_dir / "dyrenesdetektiv_kontrol_run.json",
            )
        except Exception as exc:
            logger.warning("Failed to write run metadata: %s", exc)

    logger.info("Pipeline completed in %.1fs", pipeline_run.elapsed)


if __name__ == "__main__":
    main()
