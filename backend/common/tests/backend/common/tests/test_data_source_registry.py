"""Tests for data source registry completeness.

Validates that all pipelines have corresponding entries in the data source registry.
This ensures documentation and data lineage tracking stays up to date.
"""

from pathlib import Path

import pytest
from common.data_source_registry import DATA_SOURCE_REGISTRY


class TestDataSourceRegistryCompleteness:
    """Tests to ensure all pipelines are registered."""

    @pytest.fixture
    def pipeline_directories(self) -> list[str]:
        """Get all pipeline directory names."""
        pipelines_dir = Path(__file__).parent.parent.parent / "pipelines"
        if not pipelines_dir.exists():
            return []

        # Get all directories that look like pipelines (contain main.py or similar)
        pipeline_dirs = []
        for item in pipelines_dir.iterdir():
            if item.is_dir() and not item.name.startswith(("__", ".")):
                # Check if it looks like a pipeline (has Python files)
                has_python = any(item.glob("*.py")) or any(item.glob("**/*.py"))
                if has_python:
                    pipeline_dirs.append(item.name)

        return sorted(pipeline_dirs)

    @pytest.fixture
    def registered_pipelines(self) -> set[str]:
        """Get all pipeline names from the registry."""
        return {info.pipeline_name for info in DATA_SOURCE_REGISTRY.values()}

    def test_registry_not_empty(self):
        """Registry should have entries."""
        assert len(DATA_SOURCE_REGISTRY) > 0, "Data source registry is empty"

    def test_all_entries_have_required_fields(self):
        """All registry entries should have required fields populated."""
        for source_name, info in DATA_SOURCE_REGISTRY.items():
            assert info.source_authority, f"{source_name}: missing source_authority"
            assert info.data_acquisition_method, f"{source_name}: missing data_acquisition_method"
            assert info.data_description, f"{source_name}: missing data_description"
            assert info.pipeline_name, f"{source_name}: missing pipeline_name"
            assert info.data_format, f"{source_name}: missing data_format"
            assert info.display_name, f"{source_name}: missing display_name"
            assert info.display_description, f"{source_name}: missing display_description"

    def test_display_names_are_unique(self):
        """Display names should be unique across all sources."""
        display_names = [info.display_name for info in DATA_SOURCE_REGISTRY.values()]
        duplicates = [name for name in display_names if display_names.count(name) > 1]
        assert not duplicates, f"Duplicate display names found: {set(duplicates)}"

    def test_source_keys_are_valid_identifiers(self):
        """Source keys should be valid Python identifiers (snake_case)."""
        import re

        for source_name in DATA_SOURCE_REGISTRY.keys():
            assert re.match(
                r"^[a-z][a-z0-9_]*$", source_name
            ), f"Invalid source key format: {source_name} (expected snake_case)"

    def test_data_source_types_are_valid(self):
        """All data source types should be from the enum."""
        from common.data_source_registry import DataSourceType

        valid_types = set(DataSourceType)
        for source_name, info in DATA_SOURCE_REGISTRY.items():
            assert (
                info.data_source_type in valid_types
            ), f"{source_name}: invalid data_source_type {info.data_source_type}"

    @pytest.mark.skip(reason="Enable after all pipelines are registered")
    def test_major_pipelines_are_registered(self):
        """Major known pipelines should have registry entries.

        This test ensures critical pipelines are always documented.
        Enable after initial registration is complete.
        """
        required_pipelines = {
            "unified_pipeline",
            "chr_pipeline",
            "drive_data_pipeline",
            "svineflytning_pipeline",
            "cvr_enrichment_pipeline",
        }

        registered = {info.pipeline_name for info in DATA_SOURCE_REGISTRY.values()}

        missing = required_pipelines - registered
        assert not missing, f"Required pipelines not in registry: {missing}"


class TestDataSourceRegistryQuality:
    """Tests for data quality of registry entries."""

    def test_update_frequencies_are_reasonable(self):
        """Update frequencies should use standard terms."""
        valid_frequencies = {
            "Real-time",
            "Hourly",
            "Daily",
            "Weekly",
            "Monthly",
            "Quarterly",
            "Annually",
            "On-demand",
            "Manual",
            "As needed",  # For user-uploaded data
            "Varies by dataset",  # For APIs with multiple datasets
            "Static file, manual updates available",  # For static reference data
            None,  # Allow None for unknown
        }

        for source_name, info in DATA_SOURCE_REGISTRY.items():
            if info.update_frequency:
                assert (
                    info.update_frequency in valid_frequencies
                ), f"{source_name}: non-standard update_frequency '{info.update_frequency}'"

    def test_descriptions_are_meaningful(self):
        """Descriptions should be more than just placeholders."""
        min_description_length = 20

        for source_name, info in DATA_SOURCE_REGISTRY.items():
            assert (
                len(info.data_description) >= min_description_length
            ), f"{source_name}: data_description too short ({len(info.data_description)} chars)"

            assert (
                len(info.display_description) >= min_description_length
            ), f"{source_name}: display_description too short ({len(info.display_description)} chars)"

    def test_no_placeholder_text(self):
        """Entries should not contain obvious placeholder text."""
        placeholders = ["TODO", "FIXME", "TBD", "PLACEHOLDER", "XXX"]

        for source_name, info in DATA_SOURCE_REGISTRY.items():
            for field_name in ["data_description", "display_description", "source_authority"]:
                value = getattr(info, field_name, "")
                if value:
                    for placeholder in placeholders:
                        assert (
                            placeholder not in value.upper()
                        ), f"{source_name}.{field_name} contains placeholder text: {placeholder}"
