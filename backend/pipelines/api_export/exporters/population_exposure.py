"""Publish population exposure summary JSON for the frontend."""

import json
import re

from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.population_exposure")

SUMMARY_GLOB = "gold/population_exposure_*/*/summary_*.json"
SUMMARY_PATH_RE = re.compile(
    r"(?:^|/)gold/population_exposure_(?P<year>\d{4})_(?P<field_year>\d{4})/"
    r"(?P<timestamp>[^/]+)/summary_(?P<file_year>\d{4})_(?P<file_field_year>\d{4})\.json$"
)


class PopulationExposureExporter(BaseExporter):
    """Copy the latest gold summary for each pesticide year into the static API."""

    def export(self) -> dict:
        stats: dict = {"files_written": 0, "years": [], "latest": None}

        try:
            source_paths = self.r2_glob(SUMMARY_GLOB)
        except Exception:
            logger.warning("Could not list population_exposure summaries on R2", exc_info=True)
            return stats

        latest_by_year: dict[int, tuple[str, str]] = {}
        for source_path in source_paths:
            match = SUMMARY_PATH_RE.search(source_path)
            if not match:
                logger.warning("Ignoring invalid population_exposure summary path: %s", source_path)
                continue

            year = int(match.group("year"))
            field_year = int(match.group("field_year"))
            if (
                field_year != year + 1
                or int(match.group("file_year")) != year
                or int(match.group("file_field_year")) != field_year
            ):
                logger.warning(
                    "Ignoring inconsistent population_exposure summary path: %s", source_path
                )
                continue

            timestamp = match.group("timestamp")
            previous = latest_by_year.get(year)
            if previous is None or timestamp > previous[0]:
                latest_by_year[year] = (timestamp, source_path)

        if not latest_by_year:
            logger.warning("No population_exposure summaries found on R2")
            return stats

        summaries: dict[int, dict] = {}
        for year in sorted(latest_by_year):
            source_path = latest_by_year[year][1]
            try:
                summaries[year] = self._read_summary(source_path)
            except Exception:
                logger.warning(
                    "Could not read population_exposure summary for %s from %s",
                    year,
                    source_path,
                    exc_info=True,
                )
                continue

            self.write_json(summaries[year], f"pesticides/population-exposure/summary_{year}.json")
            stats["files_written"] += 1

        years = sorted(summaries)
        if not years:
            logger.warning("No readable population_exposure summaries found on R2")
            return stats

        latest_year = years[-1]
        self.write_json(summaries[latest_year], "pesticides/population-exposure/summary.json")
        stats["files_written"] += 1

        self.write_json(
            {"years": years, "latest": latest_year},
            "pesticides/population-exposure/index.json",
        )
        stats["files_written"] += 1
        stats["years"] = years
        stats["latest"] = latest_year
        return stats

    def _read_summary(self, source_path: str) -> dict:
        """Read a gold summary JSON object from its R2 source path."""
        source_key = source_path.removeprefix("r2://")
        with self.r2_fs.open(source_key, "r") as source:
            summary = json.load(source)
        if not isinstance(summary, dict):
            raise ValueError(f"Expected a JSON object in {source_path}")
        return summary
