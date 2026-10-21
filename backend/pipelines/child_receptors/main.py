#!/usr/bin/env python3
"""CLI for bronze fetching and silver construction."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from child_receptors.bronze import (  # noqa: E402
    bbr_playgrounds,
    count_rows,
    dagtilbud,
    geofa_playgrounds,
    osm_playgrounds,
    stil_institutions,
)
from child_receptors.config import (  # noqa: E402
    BRONZE_PREFIX,
    SILVER_PREFIX,
    SOURCE_CRS,
    SOURCE_FILENAMES,
    SOURCE_URLS,
    SOURCES,
    timestamp_now,
)
from child_receptors.silver.build import DagtilbudBronzeError, build_silver, write_parquet  # noqa: E402
from child_receptors.storage import PipelineStorage, encode_manifest, silver_run_prefix, source_run_prefix  # noqa: E402

FETCHERS = {
    "dagtilbud": dagtilbud.fetch,
    "stil": stil_institutions.fetch,
    "bbr": bbr_playgrounds.fetch,
    "geofa": geofa_playgrounds.fetch,
    "osm": osm_playgrounds.fetch,
}


def _manifest(source: str, files: dict[str, bytes]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "_source": SOURCE_URLS[source],
        "_fetch_timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "_source_crs": SOURCE_CRS[source],
        "files": {name: {"bytes": len(data), "rows": count_rows(name, data)} for name, data in files.items()},
    }
    if source == "osm":
        result["licence"] = "ODbL-1.0"
    return result


def _save_bronze(
    source: str,
    timestamp: str,
    files: dict[str, bytes],
    *,
    local_dir: Path | None,
    storage: PipelineStorage | None,
) -> None:
    if set(files) != set(SOURCE_FILENAMES[source]):
        raise ValueError(f"{source} fetch returned unexpected files: {sorted(files)}")
    prefix = source_run_prefix(source, timestamp)
    if local_dir:
        run_dir = local_dir / prefix
        run_dir.mkdir(parents=True, exist_ok=True)
        for filename, content in files.items():
            (run_dir / filename).write_bytes(content)
    if storage:
        for filename, content in files.items():
            storage.upload_bytes(f"{prefix}/{filename}", content)
    manifest_bytes = encode_manifest(_manifest(source, files))
    if local_dir:
        (local_dir / prefix / "manifest.json").write_bytes(manifest_bytes)
    if storage:
        storage.upload_bytes(f"{prefix}/manifest.json", manifest_bytes)


def run_bronze(
    sources: tuple[str, ...],
    timestamp: str,
    *,
    local_dir: Path | None,
    upload: bool,
) -> None:
    storage = PipelineStorage() if upload else None
    for source in sources:
        logger.info("Fetching bronze source {}", source)
        try:
            files = FETCHERS[source]()
        except Exception as exc:
            if source not in {"dagtilbud", "osm"}:
                raise
            if source == "dagtilbud":
                logger.warning("Dagtilbudsregisteret fetch failed; skipping dagtilbud bronze for this run: {}", exc)
            else:
                logger.warning("OSM fetch failed; skipping OSM bronze for this run: {}", exc)
            continue
        _save_bronze(source, timestamp, files, local_dir=local_dir, storage=storage)
        logger.info("Saved {} bronze files for {} at {}", len(files), source, timestamp)


def _local_run(source_root: Path, source: str, timestamp: str | None) -> Path | None:
    directory = source_root / source
    if timestamp:
        candidate = directory / timestamp
        return candidate if candidate.is_dir() else None
    runs = sorted(path for path in directory.iterdir() if path.is_dir()) if directory.exists() else []
    return runs[-1] if runs else None


def _materialize_bronze(
    sources: tuple[str, ...],
    *,
    local_dir: Path | None,
    bronze_timestamp: str | None,
    storage: PipelineStorage | None,
    destination: Path,
) -> Path:
    local_root = local_dir / BRONZE_PREFIX if local_dir else None
    for source in sources:
        if source in {"dagtilbud", "osm"}:
            _materialize_fallback_runs(
                source,
                local_root=local_root,
                timestamp=bronze_timestamp,
                storage=storage,
                destination=destination,
            )
            continue
        local_run = _local_run(local_root, source, bronze_timestamp) if local_root else None
        if local_run:
            target = destination / source / local_run.name
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.resolve() != local_run.resolve():
                shutil.copytree(local_run, target, dirs_exist_ok=True)
            continue
        if storage is None:
            raise FileNotFoundError(f"No local bronze run for {source}; R2 reads are disabled")
        if bronze_timestamp:
            run_timestamp = bronze_timestamp
        else:
            timestamps = storage.list_directories(f"{BRONZE_PREFIX}/{source}")
            if not timestamps:
                raise FileNotFoundError(f"No R2 bronze run found for {source}")
            run_timestamp = timestamps[-1]
        prefix = source_run_prefix(source, run_timestamp)
        run_dir = destination / source / run_timestamp
        run_dir.mkdir(parents=True, exist_ok=True)
        for filename in (*SOURCE_FILENAMES[source], "manifest.json"):
            (run_dir / filename).write_bytes(storage.download_bytes(f"{prefix}/{filename}"))
    return destination


def _materialize_fallback_runs(
    source: str,
    *,
    local_root: Path | None,
    timestamp: str | None,
    storage: PipelineStorage | None,
    destination: Path,
) -> None:
    """Copy the requested/latest run and, if needed, its latest complete predecessor."""
    local_source_root = local_root / source if local_root else None
    locations: dict[str, list[tuple[str, Path | str]]] = {}
    if local_source_root and local_source_root.is_dir():
        for run in sorted(path for path in local_source_root.iterdir() if path.is_dir()):
            locations.setdefault(run.name, []).append(("local", run))
    if storage:
        for run_timestamp in storage.list_directories(f"{BRONZE_PREFIX}/{source}"):
            locations.setdefault(run_timestamp, []).append(("r2", run_timestamp))

    target_timestamp = timestamp or (max(locations) if locations else None)
    if target_timestamp is None:
        return

    filenames = (*SOURCE_FILENAMES[source], "manifest.json")
    require_manifest = source == "dagtilbud"

    def is_complete(location: tuple[str, Path | str]) -> bool:
        kind, value = location
        required_filenames = SOURCE_FILENAMES[source]
        if require_manifest:
            required_filenames = (*required_filenames, "manifest.json")
        if kind == "local":
            run_path = Path(value)
            return all((run_path / filename).is_file() for filename in required_filenames)
        assert storage is not None
        prefix = source_run_prefix(source, str(value))
        return all(storage.file_exists(f"{prefix}/{filename}") for filename in required_filenames)

    def preferred_location(run_timestamp: str, *, require_complete: bool) -> tuple[str, Path | str] | None:
        for location in locations.get(run_timestamp, []):
            if not require_complete or is_complete(location):
                return location
        return None

    target = preferred_location(target_timestamp, require_complete=False)
    target_complete = preferred_location(target_timestamp, require_complete=True)
    selected: list[tuple[str, tuple[str, Path | str]]] = []
    if target_complete:
        selected.append((target_timestamp, target_complete))
    else:
        if target:
            selected.append((target_timestamp, target))
        earlier_complete = [
            run_timestamp
            for run_timestamp in locations
            if run_timestamp < target_timestamp and preferred_location(run_timestamp, require_complete=True)
        ]
        if earlier_complete:
            prior_timestamp = max(earlier_complete)
            prior_location = preferred_location(prior_timestamp, require_complete=True)
            if prior_location:
                selected.append((prior_timestamp, prior_location))

    for run_timestamp, (kind, value) in selected:
        target_dir = destination / source / run_timestamp
        target_dir.mkdir(parents=True, exist_ok=True)
        for filename in filenames:
            if kind == "local":
                source_path = Path(value) / filename
                if source_path.is_file():
                    shutil.copy2(source_path, target_dir / filename)
            else:
                assert storage is not None
                relative_path = f"{source_run_prefix(source, str(value))}/{filename}"
                if storage.file_exists(relative_path):
                    (target_dir / filename).write_bytes(storage.download_bytes(relative_path))


def run_silver(
    sources: tuple[str, ...],
    *,
    local_dir: Path | None,
    bronze_timestamp: str | None,
    upload: bool,
    now: datetime | None = None,
) -> Path:
    if "dagtilbud" not in sources:
        message = (
            "Dagtilbudsregisteret is required to publish silver. "
            "Seed it locally with: cd backend/pipelines/child_receptors && "
            "python main.py --layer bronze --sources dagtilbud"
        )
        logger.critical(message)
        raise SystemExit(message)

    storage = PipelineStorage() if upload else None
    with tempfile.TemporaryDirectory(prefix="child-receptors-") as temp_dir:
        try:
            bronze_root = _materialize_bronze(
                sources,
                local_dir=local_dir,
                bronze_timestamp=bronze_timestamp,
                storage=storage,
                destination=Path(temp_dir),
            )
            records, qa_report = build_silver(
                bronze_root,
                sources=sources,
                bronze_timestamp=bronze_timestamp,
                now=now,
            )
        except DagtilbudBronzeError as exc:
            logger.critical("{}", exc)
            raise SystemExit(str(exc)) from exc
        dagtilbud_qa = qa_report.get("dagtilbud")
        if dagtilbud_qa and dagtilbud_qa["status"] == "stale":
            _report_stale_dagtilbud(dagtilbud_qa)
        timestamp = timestamp_now()
        output_root = local_dir / SILVER_PREFIX / timestamp if local_dir else Path(temp_dir) / SILVER_PREFIX / timestamp
        output_root.mkdir(parents=True, exist_ok=True)
        parquet_path = write_parquet(records, output_root / "data.parquet")
        qa_path = output_root / "qa_report.json"
        qa_path.write_text(json.dumps(qa_report, ensure_ascii=False, indent=2), encoding="utf-8")
        if storage:
            prefix = silver_run_prefix(timestamp)
            storage.upload_bytes(f"{prefix}/data.parquet", parquet_path.read_bytes())
            storage.upload_bytes(f"{prefix}/qa_report.json", qa_path.read_bytes())
            try:
                storage.enforce_retention(keep=3)
            except Exception as exc:  # Retention failure must not invalidate a successfully written version.
                logger.warning("Silver retention cleanup failed: {}", exc)
        if local_dir:
            logger.info("Silver output written to {}", output_root)
            return output_root
        logger.info("Silver output uploaded to {}", silver_run_prefix(timestamp))
        return output_root


def _report_stale_dagtilbud(qa: dict[str, Any]) -> None:
    """Publish a GitHub Actions warning and summary entry for stale daycare data."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    age_days = qa["age_days"]
    bronze_timestamp = qa["bronze_timestamp"]
    seed_command = "cd backend/pipelines/child_receptors && python main.py --layer bronze --sources dagtilbud"
    message = (
        f"::warning title=Dagtilbudsregisteret stale::{age_days} days old "
        f"(bronze {bronze_timestamp}); seed locally with: {seed_command}"
    )
    print(message)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write(message + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Danish children-sensitive receptor points")
    parser.add_argument("--layer", choices=("bronze", "silver", "all"), required=True)
    parser.add_argument("--sources", default=",".join(SOURCES), help="Comma-separated source names")
    parser.add_argument("--local-dir", type=Path, help="Local root for bronze and silver files")
    parser.add_argument("--bronze-timestamp", help="Use this timestamp for all selected bronze sources")
    parser.add_argument("--no-upload", action="store_true", help="Never read from or write to R2")
    args = parser.parse_args(argv)
    args.sources = tuple(source.strip() for source in args.sources.split(",") if source.strip())
    unknown = set(args.sources) - set(SOURCES)
    if not args.sources or unknown:
        parser.error(f"Invalid source list: {sorted(unknown) if unknown else 'empty'}")
    if args.no_upload and args.local_dir is None:
        parser.error("--no-upload requires --local-dir so outputs have a local destination")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.local_dir:
        args.local_dir.mkdir(parents=True, exist_ok=True)
    upload = not args.no_upload
    timestamp = args.bronze_timestamp or timestamp_now()
    if args.layer in {"bronze", "all"}:
        run_bronze(args.sources, timestamp, local_dir=args.local_dir, upload=upload)
    if args.layer in {"silver", "all"}:
        run_silver(
            args.sources,
            local_dir=args.local_dir,
            bronze_timestamp=args.bronze_timestamp if args.layer == "silver" else timestamp,
            upload=upload,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
