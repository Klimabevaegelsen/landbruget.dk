"""Integration tests for PMTiles Generator."""

import os
import shutil
import tempfile
from unittest.mock import AsyncMock, Mock, patch

import duckdb
import pytest

from unified_pipeline.core.gcs_data_access import GCSDataAccess
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
    config.gcs_bucket = "test-bucket"
    config.cloudflare_r2_bucket = "test-r2-bucket"
    config.r2_base_url = "https://test.example.com"
    return config


@pytest.fixture
def mock_gcs_access():
    """Create mock GCS access."""
    mock_gcs = Mock(spec=GCSDataAccess)
    mock_gcs.path_exists = AsyncMock(return_value=True)
    mock_gcs.list_paths = AsyncMock(return_value=[])
    return mock_gcs


@pytest.fixture
def duckdb_conn():
    """Create DuckDB connection for tests."""
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception:  # noqa: S110
        # Skip if spatial extension not available in test environment
        pass
    yield conn
    conn.close()


class TestDataSourceYearDetector:
    """Test year detection functionality."""

    @pytest.mark.asyncio
    async def test_detect_years_for_pattern(self, test_config, mock_gcs_access):
        """Test year detection for simple patterns."""
        # Mock GCS paths
        mock_gcs_access.list_paths.return_value = [
            "gs://test-bucket/silver/fvm_marker_2021/",
            "gs://test-bucket/silver/fvm_marker_2022/",
            "gs://test-bucket/silver/fvm_marker_2023/",
        ]

        detector = DataSourceYearDetector(test_config, mock_gcs_access)
        years = await detector._detect_years_for_pattern("fvm_marker", "silver/fvm_marker_")

        assert years == [2021, 2022, 2023]

    @pytest.mark.asyncio
    async def test_detect_pesticide_proximity_years(self, test_config, mock_gcs_access):
        """Test pesticide proximity year detection."""
        mock_gcs_access.list_paths.return_value = [
            "gs://test-bucket/gold/pesticide_proximity_2020_2021/",
            "gs://test-bucket/gold/pesticide_proximity_2021_2022/",
            "gs://test-bucket/gold/pesticide_proximity_2022_2023/",
        ]

        detector = DataSourceYearDetector(test_config, mock_gcs_access)
        years = await detector._detect_pesticide_proximity_years()

        assert years == [2020, 2021, 2022]

    @pytest.mark.asyncio
    async def test_get_years_to_process_with_target_years(self, test_config, mock_gcs_access):
        """Test year processing with explicit target years."""
        test_config.target_years = [2021, 2022]
        test_config.exclude_years = []

        detector = DataSourceYearDetector(test_config, mock_gcs_access)
        available_years = {"fvm_marker": [2020, 2021, 2022, 2023]}

        years = detector.get_years_to_process(available_years)
        assert years == [2021, 2022]

    @pytest.mark.asyncio
    async def test_get_years_to_process_with_exclusions(self, test_config, mock_gcs_access):
        """Test year processing with exclusions."""
        test_config.target_years = None
        test_config.exclude_years = [2020]

        detector = DataSourceYearDetector(test_config, mock_gcs_access)
        available_years = {"fvm_marker": [2020, 2021, 2022]}

        years = detector.get_years_to_process(available_years)
        assert years == [2021, 2022]


class TestPMTilesDataLoader:
    """Test data loading functionality."""

    @pytest.mark.asyncio
    async def test_load_fvm_marker_data(self, test_config, mock_gcs_access, duckdb_conn):
        """Test FVM marker data loading."""
        # Create test data in DuckDB
        duckdb_conn.execute("""
            CREATE TABLE test_fvm_2021 AS
            SELECT
                'field_1' as field_id,
                'block_1' as block_id,
                'CVR123' as cvr_number,
                2021 as year,
                10.5 as area_ha,
                'ST_GeomFromText(\'POINT(12.5 55.7)\')' as geometry
        """)

        # Mock GCS path existence and parquet reading
        with patch.object(duckdb_conn, "execute") as mock_execute:
            mock_execute.return_value.fetchone.return_value = [1]

            loader = PMTilesDataLoader(test_config, mock_gcs_access, duckdb_conn)
            table_name = await loader._load_fvm_marker_data(2021)

            assert table_name == "fvm_marker_2021"

    @pytest.mark.asyncio
    async def test_load_and_integrate_field_data(self, test_config, mock_gcs_access, duckdb_conn):
        """Test field data integration."""
        loader = PMTilesDataLoader(test_config, mock_gcs_access, duckdb_conn)

        # Mock individual loading methods
        with patch.object(loader, "_load_fvm_marker_data", return_value="fvm_marker_2021"):
            with patch.object(
                loader, "_load_field_environmental_analysis", return_value="field_env_2021"
            ):
                with patch.object(loader, "_load_field_production", return_value="field_prod_2021"):
                    with patch.object(
                        loader, "_integrate_field_data", return_value="integrated_2021"
                    ):
                        result = await loader.load_and_integrate_field_data(2021)
                        assert result == "integrated_2021"


class TestFieldAnalysisPMTilesGenerator:
    """Test field analysis PMTiles generation."""

    def test_build_field_analysis_query(self, test_config, mock_gcs_access, duckdb_conn):
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
                'ST_GeomFromText(\'POINT(12.5 55.7)\')' as geometry
        """)

        data_loader = PMTilesDataLoader(test_config, mock_gcs_access, duckdb_conn)
        generator = FieldAnalysisPMTilesGenerator(test_config, data_loader, duckdb_conn)

        query = generator._build_field_analysis_query("test_integrated", 2021)

        assert "field_uuid" in query
        assert "geometry" in query
        assert "WHERE geometry IS NOT NULL" in query

    def test_get_field_analysis_tippecanoe_args(self, test_config, mock_gcs_access, duckdb_conn):
        """Test tippecanoe arguments generation."""
        data_loader = PMTilesDataLoader(test_config, mock_gcs_access, duckdb_conn)
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

        with patch.object(uploader, "list_existing_pmtiles", return_value=existing_files):
            with patch.object(uploader, "delete_pmtiles", return_value=True) as mock_delete:
                cleanup_results = await uploader.cleanup_old_pmtiles(current_files, keep_versions=2)

                # Should delete old field analysis files (keeping 2 most recent: 2022, 2021)
                # Should delete old environmental file
                # Verify deletions would happen for old files
                # (keeping 2 most recent field analysis files: 2022, 2021)
                # (replacing environmental files completely)

                assert len(cleanup_results) >= 2
                assert mock_delete.call_count >= 2

    @pytest.mark.asyncio
    async def test_upload_with_cleanup(self, test_config):
        """Test upload with automatic cleanup."""
        uploader = CloudflareR2Uploader(test_config)

        files = {"pmtiles/field_analysis_2023.pmtiles": "/tmp/field_analysis_2023.pmtiles"}

        with patch.object(uploader, "upload_multiple_pmtiles") as mock_upload:
            with patch.object(uploader, "cleanup_old_pmtiles") as mock_cleanup:
                # Mock successful upload
                mock_upload.return_value = {
                    "pmtiles/field_analysis_2023.pmtiles": "https://example.com/field_analysis_2023.pmtiles"
                }
                mock_cleanup.return_value = {"pmtiles/field_analysis_2020.pmtiles": True}

                results = await uploader.upload_with_cleanup(
                    files, cleanup_old=True, keep_versions=3
                )

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

        with patch.dict(os.environ, env_vars):
            with patch("subprocess.run") as mock_run:
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
        pipeline = PMTilesGeneratorPipeline(test_config)

        # Mock setup and field generator
        with patch.object(pipeline, "setup"):
            with patch.object(pipeline, "field_generator") as mock_generator:
                mock_generator.generate_field_analysis_pmtiles.return_value = "/tmp/test.pmtiles"

                with patch.object(pipeline, "_should_upload", return_value=False):
                    result = await pipeline.generate_year_specific(2021)

                    assert result["year"] == 2021
                    assert result["success"] is True
                    assert result["field_analysis_pmtiles"] == "/tmp/test.pmtiles"

    def test_should_upload_with_credentials(self, test_config):
        """Test upload decision with credentials."""
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
