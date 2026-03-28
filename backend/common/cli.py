"""
Shared CLI utilities for pipeline entry points.

Provides reusable Click decorators, bucket resolution, metadata tracking,
and stage execution to eliminate boilerplate across pipelines.
"""

import os
import sys
import time
from pathlib import Path

import click
from common.pipeline_metadata import MetadataManager


def common_options(f):
    """Add --log-level option shared across all pipelines."""
    return click.option(
        "--log-level",
        type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
        default="INFO",
        help="Logging level (default: INFO)",
    )(f)


def stage_options(stages=None, default="all"):
    """Add --stage option with configurable choices."""
    stages = stages or ["bronze", "silver", "all"]

    def decorator(f):
        return click.option(
            "--stage",
            type=click.Choice(stages),
            default=default,
            help=f"Pipeline stage to run (default: {default})",
        )(f)

    return decorator


def resolve_bucket(override=None):
    """Resolve storage bucket from override, env vars, or default.

    Priority: override > STORAGE_BUCKET > R2_BUCKET > GCS_BUCKET > "landbruget-data"
    """
    return (
        override
        or os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET")
        or "landbruget-data"
    )


class PipelineRun:
    """Wraps MetadataManager with automatic timing.

    Replaces the 30-40 lines of metadata boilerplate repeated in every pipeline:
    - start_time tracking
    - MetadataManager initialization
    - create_metadata() with duration calculation
    - save_metadata() with directory creation
    """

    def __init__(self, source_key, logger=None):
        self.source_key = source_key
        self.logger = logger
        self.manager = MetadataManager()
        self.start_time = time.time()
        if logger:
            logger.info("Pipeline metadata system initialized")

    @property
    def elapsed(self):
        """Return elapsed time in seconds since pipeline start."""
        return time.time() - self.start_time

    def finish(self, record_count=None, file_size_bytes=None, output_path=None, source_datasets=None):
        """Create and save metadata. Call at pipeline end.

        Args:
            record_count: Number of records processed.
            file_size_bytes: Size of output file.
            output_path: Path to save metadata file alongside.
                         Parent directory is created if needed.
            source_datasets: List of source dataset names for combined datasets.

        Returns:
            Path to saved metadata file, or None if output_path not provided.
        """
        duration = self.elapsed
        metadata = self.manager.create_metadata(
            source_key=self.source_key,
            record_count=record_count,
            processing_duration=duration,
            file_size_bytes=file_size_bytes,
            source_datasets=source_datasets,
        )
        if output_path:
            p = Path(output_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            saved = self.manager.save_metadata(metadata, p)
            if self.logger:
                self.logger.info(f"Metadata saved to {saved}")
            return saved
        return None


def run_stages(stage, stages_map, logger=None, stop_on_failure=True):
    """Execute pipeline stages based on --stage argument.

    Args:
        stage: The stage to run ("bronze", "silver", "all", etc.)
        stages_map: Ordered dict of {stage_name: callable}.
                    Callables should return a truthy value on success, None on failure.
        logger: Optional logger for stage start/failure messages.
        stop_on_failure: If True and running "all", exit on first stage failure.

    Returns:
        Dict of {stage_name: result} for each executed stage.
    """
    results = {}

    for name, fn in stages_map.items():
        if stage not in [name, "all"]:
            continue

        if logger:
            logger.info(f"Starting {name} stage")

        try:
            result = fn()
            results[name] = result

            if result is None:
                if logger:
                    logger.error(f"{name} stage returned None (failed)")
                if stop_on_failure and stage == "all":
                    sys.exit(1)
        except Exception as e:
            if logger:
                logger.error(f"{name} stage failed: {e}", exc_info=True)
            results[name] = None
            if stop_on_failure:
                sys.exit(1)

    return results
