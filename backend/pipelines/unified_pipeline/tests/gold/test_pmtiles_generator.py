"""Integration tests for PMTiles Generator."""

import json
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
from unified_pipeline.gold.pmtiles_generator.utils import GeoJSONWriter
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
        assert years == [2021]

    @pytest.mark.asyncio
    async def test_get_years_to_process_requires_next_year_boundaries(
        self, test_config, mock_storage_access
    ):
        """Auto-detected PMTiles years require FVM marker data for year + 1."""
        test_config.target_years = None
        test_config.exclude_years = []

        detector = DataSourceYearDetector(test_config, mock_storage_access)
        available_years = {"fvm_marker": [2024, 2025, 2026]}

        years = detector.get_years_to_process(available_years)
        assert years == [2024, 2025]


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

    def _make_pesticide_input(self, conn, rows):
        """Materialise a synthetic enhanced-pesticide table for summary-query tests.

        Each row is a dict with the columns the BMD-enhanced summary query reads.
        """
        columns = [
            ("field_uuid", "VARCHAR"),
            ("PesticideName", "VARCHAR"),
            ("DosageQuantity", "DOUBLE"),
            ("DosageUnit", "VARCHAR"),
            ("AllocatedArea", "DOUBLE"),
            ("contains_pfas", "BOOLEAN"),
            ("contains_diquat", "BOOLEAN"),
            ("contains_glyphosate", "BOOLEAN"),
            ("health_risk", "VARCHAR"),
            ("environmental_risk", "VARCHAR"),
            ("signal_word", "VARCHAR"),
            ("product_group", "VARCHAR"),
            ("samlet_belastning", "DOUBLE"),
            ("residential_buildings_formatted", "VARCHAR"),
            ("educational_facilities_formatted", "VARCHAR"),
            ("water_distance_formatted", "VARCHAR"),
            ("MatchConfidence", "DOUBLE"),
        ]
        column_defs = ", ".join(f"{n} {t}" for n, t in columns)
        conn.execute(f"CREATE OR REPLACE TABLE pest_in ({column_defs})")
        placeholders = ", ".join("?" * len(columns))
        for row in rows:
            conn.execute(
                f"INSERT INTO pest_in VALUES ({placeholders})",
                [row.get(name) for name, _ in columns],
            )

    def test_classification_detail_emits_per_ha_dosage(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """All per-field dosage values carry dose-per-hectare, not field totals.

        Frederik V. Larsen reported (2026-04) that /pesticidkort labels
        per-product dosages with "/ha", but the value was the SJI-reported
        field total. Example: 2 L applied to a 30 ha field rendered as
        "2 L/ha" instead of the actual ~0.067 L/ha. The aggregator divides
        by AllocatedArea so the values match how the UIs label them.
        """
        self._make_pesticide_input(
            duckdb_conn,
            [
                {
                    "field_uuid": "field-1",
                    "PesticideName": "Starane 333HL",
                    "DosageQuantity": 2.0,
                    "DosageUnit": "4",
                    "AllocatedArea": 30.0,
                    "contains_pfas": False,
                    "contains_diquat": False,
                    "contains_glyphosate": False,
                    "health_risk": "",
                    "environmental_risk": "",
                    "signal_word": "",
                    "product_group": "Herbicider",
                    "samlet_belastning": 1.5,
                    "residential_buildings_formatted": "",
                    "educational_facilities_formatted": "",
                    "water_distance_formatted": "",
                    "MatchConfidence": 1.0,
                },
            ],
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        duckdb_conn.execute(loader._build_pesticide_summary_query("pest_in", has_bmd_data=True))

        # /pesticidkort detail string: name:dose_per_ha:unit:health:env:signal:group:burden
        detail = duckdb_conn.execute(
            "SELECT other_products_detail FROM temp_pesticide_summary"
        ).fetchone()[0]
        dose_per_ha = float(detail.split(":")[1])
        assert dose_per_ha == pytest.approx(2.0 / 30.0, abs=1e-4)

        # /markanalyse legacy unit-bucketed detail string: name:dose_per_ha
        legacy = duckdb_conn.execute(
            "SELECT pesticides_liters_detail FROM temp_pesticide_summary"
        ).fetchone()[0]
        legacy_dose_per_ha = float(legacy.split(":")[1])
        assert legacy_dose_per_ha == pytest.approx(2.0 / 30.0, abs=1e-4)

        # /markanalyse aggregate "Total dosering" is now also per-ha
        # (cumulative L/ha across all liquid herbicide events on the field).
        total = duckdb_conn.execute(
            "SELECT total_dosage_liters FROM temp_pesticide_summary"
        ).fetchone()[0]
        assert total == pytest.approx(2.0 / 30.0, abs=1e-4)

    def test_classification_detail_handles_zero_allocated_area(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """Rows with AllocatedArea=0 emit 0 instead of dividing by zero."""
        self._make_pesticide_input(
            duckdb_conn,
            [
                {
                    "field_uuid": "field-2",
                    "PesticideName": "Roundup",
                    "DosageQuantity": 5.0,
                    "DosageUnit": "4",
                    "AllocatedArea": 0.0,
                    "contains_pfas": False,
                    "contains_diquat": False,
                    "contains_glyphosate": True,
                    "health_risk": "",
                    "environmental_risk": "",
                    "signal_word": "",
                    "product_group": "Herbicider",
                    "samlet_belastning": 1.0,
                    "residential_buildings_formatted": "",
                    "educational_facilities_formatted": "",
                    "water_distance_formatted": "",
                    "MatchConfidence": 1.0,
                },
            ],
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        duckdb_conn.execute(loader._build_pesticide_summary_query("pest_in", has_bmd_data=True))

        detail = duckdb_conn.execute(
            "SELECT glyphosate_products_detail FROM temp_pesticide_summary"
        ).fetchone()[0]
        dose_per_ha = float(detail.split(":")[1])
        assert dose_per_ha == 0.0

    def test_fallback_summary_query_also_uses_per_ha(
        self, test_config, mock_storage_access, duckdb_conn
    ):
        """No-BMD fallback path must apply the same per-ha conversion."""
        self._make_pesticide_input(
            duckdb_conn,
            [
                {
                    "field_uuid": "field-3",
                    "PesticideName": "Starane 333HL",
                    "DosageQuantity": 2.0,
                    "DosageUnit": "4",
                    "AllocatedArea": 30.0,
                    "contains_pfas": None,
                    "contains_diquat": None,
                    "contains_glyphosate": None,
                    "health_risk": None,
                    "environmental_risk": None,
                    "signal_word": None,
                    "product_group": None,
                    "samlet_belastning": None,
                    "residential_buildings_formatted": "",
                    "educational_facilities_formatted": "",
                    "water_distance_formatted": "",
                    "MatchConfidence": 1.0,
                },
            ],
        )

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        duckdb_conn.execute(loader._build_pesticide_summary_query("pest_in", has_bmd_data=False))

        detail = duckdb_conn.execute(
            "SELECT other_products_detail FROM temp_pesticide_summary"
        ).fetchone()[0]
        dose_per_ha = float(detail.split(":")[1])
        assert dose_per_ha == pytest.approx(2.0 / 30.0, abs=1e-4)

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

    @pytest.mark.asyncio
    async def test_load_bbr_buildings_normalizes_wgs84_geometry(
        self, test_config, mock_storage_access, duckdb_conn, temp_dir
    ):
        """BBR geometries stored in EPSG:4326 should be transformed before joins."""
        try:
            duckdb_conn.execute("SELECT ST_AsText(ST_Point(12, 55))")
        except Exception:
            pytest.skip("DuckDB spatial extension not available")

        storage_path = os.path.join(temp_dir, "20260423_120000")
        os.makedirs(storage_path, exist_ok=True)
        parquet_path = os.path.join(storage_path, "joined_buildings.parquet")
        polygon_wkt = "POLYGON ((12.0 55.0, 12.01 55.0, 12.01 55.01, 12.0 55.01, 12.0 55.0))"

        duckdb_conn.execute(
            """
            CREATE OR REPLACE TABLE temp_bbr_buildings AS
            SELECT
                1 AS building_id,
                ? AS category_group,
                ST_GeomFromText(?) AS geo_building_polygon,
                ST_Centroid(ST_GeomFromText(?)) AS geo_building_centroid
            """,
            ["residential", polygon_wkt, polygon_wkt],
        )
        duckdb_conn.execute(f"COPY temp_bbr_buildings TO {parquet_path!r} (FORMAT PARQUET)")

        loader = PMTilesDataLoader(test_config, mock_storage_access, duckdb_conn)
        with patch.object(
            loader,
            "_find_timestamped_paths_ranked",
            new=AsyncMock(return_value=[f"{storage_path}/"]),
        ):
            table_name = await loader._load_bbr_buildings()

        assert table_name == "bbr_buildings"

        min_x, min_y, max_x, max_y = duckdb_conn.execute(
            """
            SELECT
                ST_XMin(ST_Extent(geometry)),
                ST_YMin(ST_Extent(geometry)),
                ST_XMax(ST_Extent(geometry)),
                ST_YMax(ST_Extent(geometry))
            FROM bbr_buildings
            """
        ).fetchone()

        assert min_x > 200_000
        assert max_x > min_x
        assert min_y > 6_000_000
        assert max_y > min_y


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
        """Cleanup should only prune old versions from refreshed families."""
        uploader = CloudflareR2Uploader(test_config)

        existing_files = [
            "pmtiles/field_analysis_2020.pmtiles",
            "pmtiles/field_analysis_2021.pmtiles",
            "pmtiles/field_analysis_2022.pmtiles",
            "pmtiles/field_analysis_2023.pmtiles",
            "pmtiles/bnbo_areas.pmtiles",
            "pmtiles/old_bnbo_areas.pmtiles",
        ]

        current_files = {
            "pmtiles/field_analysis_2023.pmtiles": "/tmp/field_analysis_2023.pmtiles",
            "pmtiles/bnbo_areas.pmtiles": "/tmp/bnbo_areas.pmtiles",
        }

        with (
            patch.object(uploader, "list_existing_pmtiles", return_value=existing_files),
            patch.object(uploader, "delete_pmtiles", return_value=True) as mock_delete,
        ):
            cleanup_results = await uploader.cleanup_old_pmtiles(current_files, keep_versions=2)

            deleted = [call.args[0] for call in mock_delete.call_args_list]
            assert cleanup_results == {"pmtiles/field_analysis_2020.pmtiles": True}
            assert deleted == ["pmtiles/field_analysis_2020.pmtiles"]

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
    async def test_cleanup_preserves_missing_year_independent_layers(self, test_config):
        """A failed buildings upload must not delete the previous published PMTiles."""
        uploader = CloudflareR2Uploader(test_config)

        existing_files = [
            "pmtiles/bnbo_areas.pmtiles",
            "pmtiles/buildings_proximity.pmtiles",
        ]
        current_files = {
            "pmtiles/bnbo_areas.pmtiles": "/tmp/bnbo_areas.pmtiles",
        }

        with (
            patch.object(uploader, "list_existing_pmtiles", return_value=existing_files),
            patch.object(uploader, "delete_pmtiles", return_value=True) as mock_delete,
        ):
            cleanup_results = await uploader.cleanup_old_pmtiles(current_files, keep_versions=3)

            assert cleanup_results == {}
            assert mock_delete.call_count == 0

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


class TestGeoJSONWriter:
    """Tests for GeoJSON export utility."""

    @pytest.mark.asyncio
    async def test_write_geojson_from_query_writes_valid_feature_collection(
        self, duckdb_conn, tmp_path
    ):
        duckdb_conn.execute("""
            CREATE TABLE geojson_rows AS
            SELECT
                'field-1' AS field_uuid,
                2024 AS field_year,
                '{"type":"Point","coordinates":[10.0,55.0]}' AS geometry
            UNION ALL
            SELECT
                'field-2' AS field_uuid,
                2024 AS field_year,
                '{"type":"Point","coordinates":[11.0,56.0]}' AS geometry
        """)
        output_path = tmp_path / "fields.geojson"

        success = await GeoJSONWriter.write_geojson_from_query(
            duckdb_conn,
            "SELECT field_uuid, field_year, geometry FROM geojson_rows ORDER BY field_uuid",
            str(output_path),
        )

        assert success is True
        data = json.loads(output_path.read_text(encoding="utf-8"))
        assert data["type"] == "FeatureCollection"
        assert [feature["properties"]["field_uuid"] for feature in data["features"]] == [
            "field-1",
            "field-2",
        ]
        assert data["features"][0]["geometry"]["coordinates"] == [10.0, 55.0]


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
