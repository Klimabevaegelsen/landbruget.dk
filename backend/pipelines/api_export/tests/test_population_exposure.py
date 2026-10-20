"""Tests for the population exposure summary exporter."""

import ast
import json
from pathlib import Path

import duckdb

from exporters import population_exposure as population_exposure_module
from exporters.population_exposure import PopulationExposureExporter


class _StubPopulationExposureExporter(PopulationExposureExporter):
    def __init__(self, *args, summaries: dict[str, dict], paths: list[str], **kwargs):
        super().__init__(*args, **kwargs)
        self.summaries = summaries
        self.paths = paths

    def r2_glob(self, pattern: str) -> list[str]:
        assert pattern == "gold/population_exposure_*/*/summary_*.json"
        return self.paths

    def _read_summary(self, source_path: str) -> dict:
        return self.summaries[source_path]


def _source(year: int, timestamp: str) -> str:
    return (
        f"r2://landbruget-data/gold/population_exposure_{year}_{year + 1}/"
        f"{timestamp}/summary_{year}_{year + 1}.json"
    )


def test_export_publishes_latest_summary_per_year_and_index(tmp_path: Path) -> None:
    old_2023 = _source(2023, "2026-10-20T010000Z")
    new_2023 = _source(2023, "2026-10-20T020000Z")
    summary_2024 = _source(2024, "2026-10-20T030000Z")
    payloads = {
        old_2023: {"pesticide_year": 2023, "marker": "old"},
        new_2023: {"pesticide_year": 2023, "marker": "latest", "nested": [1, 2]},
        summary_2024: {"pesticide_year": 2024, "marker": "latest year"},
    }
    exporter = _StubPopulationExposureExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        paths=[old_2023, summary_2024, new_2023],
        summaries=payloads,
    )

    stats = exporter.export()

    output = tmp_path / "pesticides" / "population-exposure"
    assert json.loads((output / "summary_2023.json").read_text()) == payloads[new_2023]
    assert json.loads((output / "summary_2024.json").read_text()) == payloads[summary_2024]
    assert json.loads((output / "summary.json").read_text()) == payloads[summary_2024]
    assert json.loads((output / "index.json").read_text()) == {
        "years": [2023, 2024],
        "latest": 2024,
    }
    assert stats == {"files_written": 4, "years": [2023, 2024], "latest": 2024}


def test_export_skips_missing_gold_without_failing(tmp_path: Path, monkeypatch) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(
        population_exposure_module.logger,
        "warning",
        lambda message, *args, **kwargs: warnings.append(message),
    )
    exporter = _StubPopulationExposureExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        paths=[],
        summaries={},
    )

    stats = exporter.export()

    assert stats == {"files_written": 0, "years": [], "latest": None}
    assert not (tmp_path / "pesticides").exists()
    assert any("No population_exposure summaries found" in warning for warning in warnings)


def test_export_ignores_invalid_summary_paths(tmp_path: Path) -> None:
    exporter = _StubPopulationExposureExporter(
        conn=duckdb.connect(":memory:"),
        output_dir=str(tmp_path),
        paths=["r2://landbruget-data/gold/population_exposure_2024_2025/not-the-summary.json"],
        summaries={},
    )

    stats = exporter.export()

    assert stats == {"files_written": 0, "years": [], "latest": None}


def test_population_exposure_exporter_is_registered() -> None:
    main_path = Path(__file__).parents[1] / "main.py"
    module = ast.parse(main_path.read_text())
    registry = next(
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "EXPORTERS" for target in node.targets
        )
    )

    assert isinstance(registry.value, ast.Dict)
    registrations = {
        key.value: value.id
        for key, value in zip(registry.value.keys, registry.value.values, strict=True)
        if isinstance(key, ast.Constant) and isinstance(value, ast.Name)
    }
    assert registrations["population_exposure"] == "PopulationExposureExporter"
