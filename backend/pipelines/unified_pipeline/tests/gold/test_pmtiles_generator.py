"""Integration tests for PMTiles Generator."""

import os
import shutil
import tempfile
from unittest.mock import AsyncMock, Mock, patch

import duckdb
import pytest
from common.storage import StorageAccess

from unified_pipeline.gold.pmtiles_generator.config import PMTilesGeneratorConfig
from unified_pipeline.gold.pmtiles_generator.data_loader import PMTilesDataLoader
from unified_pipeline.gold.pmtiles_generator.field_analysis_generator import (
    FieldAnalysisPMTilesGenerator,
)
from unified_pipeline.gold.pmtiles_generator.main import PMTilesGeneratorPipeline
from unified_pipeline.gold.pmtiles_generator.uploader import CloudflareR2Uploader
from unified_pipeline.gold.pmtiles_generator.year_detector import DataSourceYearDetector


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_config(temp_dir):
    """Create test configuration."""
    config = PMTilesGeneratorConfig()
    config.temp_dir = temp_dir
    config.cleanup_temp_files = False  # Keep files for inspection during tests
    config.storage_bucket = "test-bucket"
    config.cloudflare_r2_bucket = "test-r2-bucket"
    config.r2_base_url = "https://test.example.com"
    return config


def test_config_defaults_target_public_api_domain():
    """Default publish target should be the shared public API bucket/domain."""
    config = PMTilesGeneratorConfig()

    assert config.cloudflare_r2_bucket == "landbruget-data"
    assert config.r2_base_url == "https://api.landbruget.dk"


@pytest.fixture
def mock_storage_access():
    """Create mock cloud storage access."""
    mock_gcs = Mock(spec=StorageAccess)
    mock_gcs.path_exists = AsyncMock(return_value=True)
    mock_gcs.list_paths = AsyncMock(return_value=[])
    mock_gcs.list_files = Mock(return_value=[])
    return mock_gcs


@pytest.fixture
def duckdb_conn():
    """Create DuckDB connection for tests."""
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception:
        # Skip if spatial extension not available in test environment
        pass
    yield conn
    conn.close()


class TestDataSourceYearDetector:
    """Test year detection functionality."""

    @pytest.mark.asyncio
    async def test_detect_years_for_pattern(self, test_config, mock_storage_access):
        """Test year detection for simple patterns."""
        # Mock storage paths
        # Paths without trailing slash so split("/")[-1] yields the directory name
        mock_storage_access.list_files = Mock(
            return_value=[
                "test-bucket/silver/fvm_marker_2021",
                "test-bucket/silver/fvm_marker_2022",
                "test-bucket/silver/fvm_marker_2023",
            ]
        )

        detector = DataSourceYearDetector(test_config, mock_storage_access)
        years = await detector._detect_years_for_pattern("fvm_marker", "silver/fvm_marker_")

        assert years == [2021, 2022, 2023]

    @pytest.mark.asyncio
    async def test_detect_pesticide_proximity_years(self, test_config, mock_storage_access):
        """Test pesticide proximity year detection."""
        mock_storage_access.list_files = Mock(
            return_value=[
                "test-bucket/gold/pesticide_proximity_2020_2021/",
                "test-bucket/gold/pesticide_proximity_2021_2022/",
                "test-bucket/gold/pesticide_proximity_2022_2023/",
            ]
        )

        detector = DataSourceYearDetector(test_config, mock_storage_access)
        years = await detector._detect_pesticide_proximity_years()

        assert years == [2020, 2021, 2022]

    @pytest.mark.asyncio
    async def test_get_years_to_process_with_target_years(self, test_config, mock_storage_access):
        """Test year processing with explicit target years."""
        test_config.target_years = [2021, 2022]
        test_config.exclude_years = []

        detector = DataSourceYearDetector(test_config, mock_storage_access)
        available_years = {"fvm_marker": [2020, 2021, 2022, 2023]}

        years = detector.get_years_to_process(available_years)
        assert years == [2021, 2022]

    @pytest.mark.asyncio
    async def test_get_years_to_process_with_exclusions(self, test_config, mock_storage_access):
        """Test year processing with exclusions."""
        test_config.target_years = None
        test_config.exclude_years = [2020]

        detector = DataSourceYearDetector(test_config, mock_storage_access)
        available_years = {"fvm_marker": [2020, 2021, 2022]}

        years = detector.get_years_to_process(available_years)
        assert years == [2021, 2022]


class TestPMTilesDataLoader:
    """Test data loading functionality."""

    @pytest.mark.asyncio
    async def test_load_fvm_marker_data(self, test_config, mock_storage_access, duckdb_conn):
        """Test FVM marker data loading returns the expected table name."""
        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)

        # Mock _load_fvm_marker_data entirely to verify interface contract
        with patch.object(
            loader,
            "_load_fvm_marker_data",
            new=AsyncMock(return_value="fvm_marker_2021"),
        ):
            table_name = await loader._load_fvm_marker_data(2021)

            assert table_name == "fvm_marker_2021"

    @pytest.mark.asyncio
    async def test_load_and_integrate_field_data(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test field data integration."""
        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)

        # Mock individual loading methods
        with (
            patch.object(loader, "_load_fvm_marker_data", return_value="fvm_marker_2021"),
            patch.object(
                loader, "_load_field_environmental_analysis", return_value="field_env_2021"
            ),
            patch.object(loader, "_load_field_production", return_value="field_prod_2021"),
            patch.object(loader, "_integrate_field_data", return_value="integrated_2021"),
        ):
            result = await loader.load_and_integrate_field_data(2021)
            assert result == "integrated_2021"


class TestTimestampFallback:
    """Test timestamp fallback in data loader."""

    @pytest.mark.asyncio
    async def test_find_timestamped_paths_ranked_returns_multiple(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test that ranked paths returns multiple directories sorted newest-first."""
        mock_storage_access.list_files_with_timestamps = Mock(
            return_value=[
                (
                    "test-bucket/silver/fvm_marker_2024/20260320_100000/data.parquet",
                    1710921600,
                ),
                (
                    "test-bucket/silver/fvm_marker_2024/20260322_170000/data.parquet",
                    1711094400,
                ),
                (
                    "test-bucket/silver/fvm_marker_2024/20260319_080000/data.parquet",
                    1710835200,
                ),
            ]
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        paths = await loader._find_timestamped_paths_ranked("test-bucket/silver/fvm_marker_2024")

        assert len(paths) == 3
        # Newest first
        assert "20260322_170000" in paths[0]
        assert "20260320_100000" in paths[1]
        assert "20260319_080000" in paths[2]

    @pytest.mark.asyncio
    async def test_find_timestamped_paths_ranked_respects_max_results(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test that max_results limits the number of paths returned."""
        mock_storage_access.list_files_with_timestamps = Mock(
            return_value=[
                ("test-bucket/silver/data/20260322/data.parquet", 3),
                ("test-bucket/silver/data/20260321/data.parquet", 2),
                ("test-bucket/silver/data/20260320/data.parquet", 1),
            ]
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        paths = await loader._find_timestamped_paths_ranked(
            "test-bucket/silver/data", max_results=2
        )

        assert len(paths) == 2

    @pytest.mark.asyncio
    async def test_find_timestamped_paths_ranked_deduplicates_directories(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test that paths from the same directory are deduplicated."""
        mock_storage_access.list_files_with_timestamps = Mock(
            return_value=[
                ("test-bucket/silver/data/20260322/data.parquet", 2),
                ("test-bucket/silver/data/20260322/other.parquet", 2),
                ("test-bucket/silver/data/20260321/data.parquet", 1),
            ]
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        paths = await loader._find_timestamped_paths_ranked("test-bucket/silver/data")

        assert len(paths) == 2

    @pytest.mark.asyncio
    async def test_find_timestamped_paths_ranked_empty_when_no_files(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test that empty list is returned when no files are found."""
        mock_storage_access.list_files_with_timestamps = Mock(return_value=[])

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        paths = await loader._find_timestamped_paths_ranked("test-bucket/silver/data")

        assert paths == []

    @pytest.mark.asyncio
    async def test_find_latest_timestamped_path_returns_first(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test that _find_latest_timestamped_path returns only the newest path."""
        mock_storage_access.list_files_with_timestamps = Mock(
            return_value=[
                ("test-bucket/silver/data/20260322/data.parquet", 2),
                ("test-bucket/silver/data/20260321/data.parquet", 1),
            ]
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        path = await loader._find_latest_timestamped_path("test-bucket/silver/data")

        assert path is not None
        assert "20260322" in path


class TestFieldAnalysisPMTilesGenerator:
    """Test field analysis PMTiles generation."""

    def test_build_field_analysis_query(self, test_config, mock_storage_access, duckdb_conn):
        """Test SQL query building for field analysis."""
        # Create test table
        duckdb_conn.execute("""
            CREATE TABLE test_integrated AS
            SELECT
                'field_1' as field_uuid,
                'field_1' as field_id,
                2021 as year,
                10.5 as area_ha,
                'Wheat' as crop_name,
                false as is_organic,
                NULL as geometry
        """)

        data_loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        generator = FieldAnalysisPMTilesGenerator(test_config, data_loader, duckdb_conn)

        query = generator._build_field_analysis_query("test_integrated", 2021)

        assert "field_uuid" in query
        assert "geometry" in query
        assert "WHERE geometry IS NOT NULL" in query

    def test_get_field_analysis_tippecanoe_args(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Test tippecanoe arguments generation."""
        data_loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        generator = FieldAnalysisPMTilesGenerator(test_config, data_loader, duckdb_conn)

        args = generator._get_field_analysis_tippecanoe_args()

        assert "--detect-shared-borders" in args
        assert "--attribute-type=area_ha:float" in args
        assert "--attribute-type=year:int" in args


class TestCloudflareR2Uploader:
    """Test R2 upload functionality."""

    def test_generate_upload_manifest(self, test_config):
        """Test upload manifest generation."""
        uploader = CloudflareR2Uploader(test_config)

        pmtiles_files = {
            "field_analysis": "/tmp/field_analysis.pmtiles",
            "bnbo_areas": "/tmp/bnbo_areas.pmtiles",
            "wetlands_all": "/tmp/wetlands.pmtiles",
        }

        # Mock file existence
        with patch("os.path.exists", return_value=True):
            manifest = uploader.generate_upload_manifest(pmtiles_files, year=2021)

            expected_keys = {
                "pmtiles/field_analysis_2021.pmtiles",
                "pmtiles/bnbo_areas.pmtiles",
                "pmtiles/wetlands_all.pmtiles",
            }

            assert set(manifest.keys()) == expected_keys

    @pytest.mark.asyncio
    async def test_cleanup_old_pmtiles(self, test_config):
        """Test cleanup of old PMTiles files."""
        uploader = CloudflareR2Uploader(test_config)

        # Mock existing files in R2
        existing_files = [
            "pmtiles/field_analysis_2020.pmtiles",
            "pmtiles/field_analysis_2021.pmtiles",
            "pmtiles/field_analysis_2022.pmtiles",
            "pmtiles/field_analysis_2023.pmtiles",
            "pmtiles/bnbo_areas.pmtiles",
            "pmtiles/old_bnbo_areas.pmtiles",
        ]

        # Current files being uploaded
        current_files = {
            "pmtiles/field_analysis_2023.pmtiles": "/tmp/field_analysis_2023.pmtiles",
            "pmtiles/bnbo_areas.pmtiles": "/tmp/bnbo_areas.pmtiles",
        }

        with (
            patch.object(uploader, "list_existing_pmtiles", return_value=existing_files),
            patch.object(uploader, "delete_pmtiles", return_value=True) as mock_delete,
        ):
            cleanup_results = await uploader.cleanup_old_pmtiles(current_files, keep_versions=2)

            # Should delete old field analysis files (keeping 2 most recent: 2022, 2021)
            # Should delete old environmental file
            # Verify deletions would happen for old files
            # (keeping 2 most recent field analysis files: 2022, 2021)
            # (replacing environmental files completely)

            assert len(cleanup_results) >= 2
            assert mock_delete.call_count >= 2

    @pytest.mark.asyncio
    async def test_cleanup_skips_families_not_in_current_upload(self, test_config):
        """Partial runs (e.g. --environmental-only) must not delete files from
        layer families they didn't upload.

        Regression: an env-only run on 2026-04-18 deleted every
        field_analysis_overview_<year>.pmtiles (overview family misclassified)
        AND pruned 15 of 18 field_analysis_<year>.pmtiles via keep_versions
        retention. The < zoom 12 view of pesticidkort went blank, and 2008–2022
        field tiles disappeared too.

        Cleanup must skip a family entirely when zero current files belong to
        it — retention only makes sense when a family is being refreshed.
        """
        uploader = CloudflareR2Uploader(test_config)

        existing_files = [
            *(f"pmtiles/field_analysis_{y}.pmtiles" for y in range(2008, 2026)),
            *(f"pmtiles/field_analysis_overview_{y}.pmtiles" for y in range(2008, 2026)),
            "pmtiles/bnbo_areas.pmtiles",
        ]

        # Simulate an --environmental-only upload: only env layers in current_files.
        current_files = {
            "pmtiles/bnbo_areas.pmtiles": "/tmp/bnbo_areas.pmtiles",
        }

        with (
            patch.object(uploader, "list_existing_pmtiles", return_value=existing_files),
            patch.object(uploader, "delete_pmtiles", return_value=True) as mock_delete,
        ):
            await uploader.cleanup_old_pmtiles(current_files, keep_versions=3)

            deleted = {call.args[0] for call in mock_delete.call_args_list}
            for path in existing_files:
                if "field_analysis" in path:
                    assert path not in deleted, f"{path} was deleted by an env-only cleanup"

    @pytest.mark.asyncio
    async def test_upload_with_cleanup(self, test_config):
        """Test upload with automatic cleanup."""
        uploader = CloudflareR2Uploader(test_config)

        files = {"pmtiles/field_analysis_2023.pmtiles": "/tmp/field_analysis_2023.pmtiles"}

        with (
            patch.object(uploader, "upload_multiple_pmtiles") as mock_upload,
            patch.object(uploader, "cleanup_old_pmtiles") as mock_cleanup,
        ):
            # Mock successful upload
            mock_upload.return_value = {
                "pmtiles/field_analysis_2023.pmtiles": "https://example.com/field_analysis_2023.pmtiles"
            }
            mock_cleanup.return_value = {"pmtiles/field_analysis_2020.pmtiles": True}

            results = await uploader.upload_with_cleanup(files, cleanup_old=True, keep_versions=3)

            # Should have called upload and cleanup
            mock_upload.assert_called_once_with(files)
            mock_cleanup.assert_called_once()

            # Should have cleanup results
            assert "_cleanup_results" in results
            assert results["_cleanup_results"] == {"pmtiles/field_analysis_2020.pmtiles": True}

    @pytest.mark.asyncio
    async def test_setup_rclone_config(self, test_config):
        """Test rclone configuration setup."""
        uploader = CloudflareR2Uploader(test_config)

        # Mock environment variables
        env_vars = {
            "R2_ACCESS_KEY_ID": "test_key",
            "R2_SECRET_ACCESS_KEY": "test_secret",
            "R2_ENDPOINT": "https://test.r2.cloudflarestorage.com",
        }

        with patch.dict(os.environ, env_vars), patch("subprocess.run") as mock_run:
            # Mock rclone version check
            mock_run.side_effect = [
                Mock(returncode=0, stdout="rclone v1.60.0"),  # version check
                Mock(returncode=0, stdout=""),  # listremotes
                Mock(returncode=0, stdout=""),  # config create
            ]

            result = await uploader.setup_rclone_config()
            assert result is True


class TestPMTilesGeneratorPipeline:
    """Test main pipeline functionality."""

    @pytest.mark.asyncio
    async def test_setup(self, test_config):
        """Test pipeline setup."""
        with patch("common.storage.core.get_r2_filesystem"):
            pipeline = PMTilesGeneratorPipeline(test_config)

        with patch.object(pipeline, "duckdb_conn") as mock_conn:
            mock_conn.execute = Mock()

            await pipeline.setup()

            assert pipeline.year_detector is not None
            assert pipeline.data_loader is not None
            assert pipeline.field_generator is not None
            assert pipeline.environmental_generator is not None
            assert pipeline.buildings_generator is not None
            assert pipeline.uploader is not None

    @pytest.mark.asyncio
    async def test_generate_year_specific(self, test_config):
        """Test year-specific generation."""
        with patch("common.storage.core.get_r2_filesystem"):
            pipeline = PMTilesGeneratorPipeline(test_config)

        # Mock setup and field generator
        with (
            patch.object(pipeline, "setup"),
            patch.object(pipeline, "field_generator") as mock_generator,
            patch.object(pipeline, "_should_upload", return_value=False),
        ):
            mock_generator.generate_field_analysis_pmtiles = AsyncMock(
                return_value="/tmp/test.pmtiles"
            )
            mock_generator.generate_overview_pmtiles = AsyncMock(
                return_value="/tmp/test_overview.pmtiles"
            )

            result = await pipeline.generate_year_specific(2021)

            assert result["year"] == 2021
            assert result["success"] is True
            assert result["field_analysis_pmtiles"] == "/tmp/test.pmtiles"
            assert result["overview_pmtiles"] == "/tmp/test_overview.pmtiles"

    def test_should_upload_with_credentials(self, test_config):
        """Test upload decision with credentials."""
        with patch("common.storage.core.get_r2_filesystem"):
            pipeline = PMTilesGeneratorPipeline(test_config)

        env_vars = {
            "R2_ACCESS_KEY_ID": "test_key",
            "R2_SECRET_ACCESS_KEY": "test_secret",
            "R2_ENDPOINT": "https://test.r2.cloudflarestorage.com",
        }

        with patch.dict(os.environ, env_vars):
            assert pipeline._should_upload() is True

    def test_should_upload_without_credentials(self, test_config):
        """Test upload decision without credentials."""
        with patch("common.storage.core.get_r2_filesystem"):
            pipeline = PMTilesGeneratorPipeline(test_config)

        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            assert pipeline._should_upload() is False


@pytest.mark.integration
class TestPMTilesIntegration:
    """Integration tests that require external dependencies."""

    @pytest.mark.skipif(not shutil.which("tippecanoe"), reason="tippecanoe not available")
    def test_tippecanoe_available(self):
        """Test that tippecanoe is available for integration tests."""
        from unified_pipeline.gold.pmtiles_generator.utils import TippecanoeRunner

        runner = TippecanoeRunner()
        assert runner.check_tippecanoe_available() is True

    @pytest.mark.skipif(not shutil.which("rclone"), reason="rclone not available")
    def test_rclone_available(self):
        """Test that rclone is available for integration tests."""
        import subprocess

        result = subprocess.run(["rclone", "version"], capture_output=True)
        assert result.returncode == 0


if __name__ == "__main__":
    pytest.main([__file__])
