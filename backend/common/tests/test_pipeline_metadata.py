"""Tests for pipeline metadata utilities.

Tests the metadata creation, persistence, and serialization
used for tracking data provenance across all data pipelines.
"""

import json

import pytest
from common.pipeline_metadata import (
    DatasetMetadata,
    MetadataManager,
)


@pytest.fixture()
def metadata_manager():
    """Create a MetadataManager instance for testing."""
    return MetadataManager()


class TestMetadataManagerCreation:
    """Test MetadataManager.create_metadata method."""

    def test_create_metadata_returns_dataset_metadata(self, metadata_manager):
        """create_metadata should return a DatasetMetadata instance."""
        metadata = metadata_manager.create_metadata("cadastral")

        assert isinstance(metadata, DatasetMetadata)
        assert metadata.source_info is not None
        assert metadata.processing is not None

    def test_create_metadata_with_record_count(self, metadata_manager):
        """create_metadata should set record_count when provided."""
        metadata = metadata_manager.create_metadata("cadastral", record_count=42000)

        assert metadata.processing.record_count == 42000

    def test_create_metadata_with_processing_duration(self, metadata_manager):
        """create_metadata should set processing_duration_seconds when provided."""
        metadata = metadata_manager.create_metadata("cadastral", processing_duration=123.45)

        assert metadata.processing.processing_duration_seconds == 123.45

    def test_create_metadata_with_file_size(self, metadata_manager):
        """create_metadata should set file_size_bytes when provided."""
        metadata = metadata_manager.create_metadata("cadastral", file_size_bytes=1024000)

        assert metadata.processing.file_size_bytes == 1024000

    def test_create_metadata_with_source_datasets(self, metadata_manager):
        """create_metadata should set source_datasets for combined datasets."""
        metadata = metadata_manager.create_metadata(
            "cadastral",
            source_datasets=["cadastral_2023", "cadastral_2024"],
        )

        assert metadata.source_datasets == ["cadastral_2023", "cadastral_2024"]

    def test_create_metadata_invalid_source_key(self, metadata_manager):
        """create_metadata should raise ValueError for unknown source keys."""
        with pytest.raises(ValueError, match="not found in registry"):
            metadata_manager.create_metadata("nonexistent_source_key_xyz")

    def test_create_metadata_populates_source_info(self, metadata_manager):
        """create_metadata should populate source_info from the registry."""
        metadata = metadata_manager.create_metadata("cadastral")

        assert metadata.source_info.source_authority == "Geodatastyrelsen"
        assert metadata.source_info.pipeline_name == "unified_pipeline"
        assert metadata.source_info.display_name == "Matrikelgrænser"


class TestMetadataManagerPersistence:
    """Test save and load metadata functionality."""

    def test_save_metadata_creates_file(self, metadata_manager, tmp_path):
        """save_metadata should create a JSON metadata file alongside the data file."""
        metadata = metadata_manager.create_metadata("cadastral", record_count=100)
        data_file = tmp_path / "cadastral_gold.parquet"
        data_file.touch()

        metadata_path = metadata_manager.save_metadata(metadata, data_file)

        assert metadata_path.exists()
        assert metadata_path.name == "cadastral_gold_metadata.json"
        assert metadata_path.parent == tmp_path

        # Verify it is valid JSON
        content = json.loads(metadata_path.read_text())
        assert isinstance(content, dict)

    def test_load_metadata_roundtrip(self, metadata_manager, tmp_path):
        """Saving then loading metadata should produce an equivalent object."""
        original = metadata_manager.create_metadata("cadastral", record_count=5000)
        data_file = tmp_path / "test_data.parquet"
        data_file.touch()

        metadata_path = metadata_manager.save_metadata(original, data_file)
        loaded = metadata_manager.load_metadata(metadata_path)

        assert loaded.source_info.source_authority == original.source_info.source_authority
        assert loaded.source_info.pipeline_name == original.source_info.pipeline_name
        assert loaded.processing.record_count == original.processing.record_count
        assert loaded.processing.environment == original.processing.environment


class TestProcessingMetadataEnvironment:
    """Test automatic environment detection."""

    def test_processing_metadata_auto_detects_local_environment(self, metadata_manager, monkeypatch):
        """MetadataManager should detect 'local' environment by default."""
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)

        manager = MetadataManager()
        assert manager.environment == "local"

    def test_processing_metadata_detects_prod_on_github_actions(self, monkeypatch):
        """MetadataManager should detect 'prod' environment when GITHUB_ACTIONS is set."""
        monkeypatch.setenv("GITHUB_ACTIONS", "true")

        manager = MetadataManager()
        assert manager.environment == "prod"

    def test_processing_metadata_detects_dev_environment(self, monkeypatch):
        """MetadataManager should detect 'dev' environment when ENVIRONMENT=dev."""
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        monkeypatch.setenv("ENVIRONMENT", "dev")

        manager = MetadataManager()
        assert manager.environment == "dev"


class TestDatasetMetadataFrontendDisplay:
    """Test the to_frontend_display serialization."""

    def test_dataset_metadata_to_frontend_display(self, metadata_manager):
        """to_frontend_display should return a dict with expected keys."""
        metadata = metadata_manager.create_metadata("cadastral", record_count=1234)

        display = metadata.to_frontend_display()

        assert isinstance(display, dict)
        assert "source" in display
        assert "date_of_processing" in display
        assert "code_version" in display
        assert "display_name" in display
        assert "display_description" in display
        assert "record_count" in display
        assert "environment" in display

        assert display["source"] == "Geodatastyrelsen"
        assert display["display_name"] == "Matrikelgrænser"
        assert display["record_count"] == 1234

    def test_dataset_metadata_to_frontend_display_includes_all_fields(self, metadata_manager):
        """to_frontend_display should include all documented fields."""
        metadata = metadata_manager.create_metadata(
            "cadastral",
            record_count=500,
            processing_duration=60.5,
            file_size_bytes=2048,
            source_datasets=["ds1", "ds2"],
        )

        display = metadata.to_frontend_display()

        expected_keys = {
            "source",
            "date_of_acquisition",
            "date_of_processing",
            "code_version",
            "documentation",
            "display_name",
            "display_description",
            "update_frequency",
            "record_count",
            "source_datasets",
            "data_acquisition_method",
            "processing_duration",
            "environment",
            "file_size_bytes",
        }
        assert set(display.keys()) == expected_keys
        assert display["source_datasets"] == ["ds1", "ds2"]
        assert display["processing_duration"] == 60.5
        assert display["file_size_bytes"] == 2048
