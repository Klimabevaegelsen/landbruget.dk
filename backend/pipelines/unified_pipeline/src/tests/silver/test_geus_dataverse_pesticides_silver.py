"""
Tests for the GEUSDataversePesticidesSilver class.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from unified_pipeline.silver.geus_dataverse_pesticides import (
    GEUSDataversePesticidesSilver,
    GEUSDataversePesticidesSilverConfig,
)


@pytest.fixture
def config() -> GEUSDataversePesticidesSilverConfig:
    """Return a test configuration."""
    return GEUSDataversePesticidesSilverConfig(
        dataset="test_geus_dataverse_pesticides",
        bucket="test-bucket",
    )


@pytest.fixture
def silver_source(
    config: GEUSDataversePesticidesSilverConfig,
) -> GEUSDataversePesticidesSilver:
    """Return a test GEUSDataversePesticidesSilver instance."""
    source = GEUSDataversePesticidesSilver(config)
    source.log = MagicMock()
    return source


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Return a sample DataFrame mimicking AM_pest.rds structure (34 columns)."""
    return pd.DataFrame(
        {
            "PROEVEAAR": [2020.0, 2020.0, 2021.0, 2021.0, 2022.0],
            "STOFKODE": [438.0, 846.0, 438.0, 1448.0, 2251.0],
            "STOFNAVN": [
                "2,6-Dichlorbenzamid",
                "Atrazin",
                "2,6-Dichlorbenzamid",
                "Desphenyl chloridazon",
                "Trifluoreddikesyre (TFA)",
            ],
            "AM": [0.15, 0.05, 0.20, 0.08, 0.12],
            "XUTM32EUREF89": [725000.0, 726000.0, 727000.0, 725500.0, 726500.0],
            "YUTM32EUREF89": [6175000.0, 6176000.0, 6177000.0, 6175500.0, 6176500.0],
            "DGUNR": ["123.456", "234.567", "345.678", "456.789", "567.890"],
            "BORID": [100275, 100276, 100277, 100278, 100279],
            "INDTAGSID": ["1", "1", "2", "1", "1"],
            "BORINGSDYBDE": [59.0, 45.0, 72.0, 38.0, 55.0],
            "TERRAENKOTE": [84.59, 72.3, 91.2, 65.8, 78.4],
            "INDTAG_TOP": [48.0, 35.0, 60.0, 28.0, 45.0],
            "INDTAG_BUND": [57.0, 43.0, 70.0, 36.0, 53.0],
            "Grundvandsforekomst": [
                "DK111_dkmj_1090_ks",
                "DK111_dkmj_1091_ks",
                "DK111_dkmj_1092_ks",
                "DK111_dkmj_1093_ks",
                "DK111_dkmj_1094_ks",
            ],
            "REDOXVANDTYPE": ["C", "A", "B", "C", "A"],
            "Depth": ["Unknown", "Shallow", "Deep", "Unknown", "Shallow"],
            "DATATYPE": ["GRUMO", "GRUMO", "VF", "DEPOT", "GRUMO"],
            "euProgrammeCode": ["DKGRUMO", "DKGRUMO", "DKVandforsyning", "DKForurening", "DKGRUMO"],
            "euMonitoringSiteCode": [
                "DK115-1106-1",
                "DK115-1107-1",
                "DK115-1108-2",
                "DK115-1109-1",
                "DK115-1110-1",
            ],
            "CASNUMBER": ["2008-58-4", "1912-24-9", "2008-58-4", "6339-19-1", "76-05-1"],
            "ENHED": ["20", "20", "20", "20", "20"],
        }
    )


@pytest.fixture
def sample_pfas_dataframe() -> pd.DataFrame:
    """Return a sample DataFrame mimicking AM_pfas.rds structure."""
    return pd.DataFrame(
        {
            "PROEVEAAR": [2020.0, 2020.0, 2021.0, 2021.0, 2022.0],
            "KORT_NAVN": ["PFOS", "PFOA", "PFNA", "PFHxS", "PFBS"],
            "GROUP": [
                "Perfluoroalkyl sulfonates",
                "Perfluorocarboxylic acids",
                "Perfluorocarboxylic acids",
                "Perfluoroalkyl sulfonates",
                "Perfluoroalkyl sulfonates",
            ],
            "AM": [2.5, 1.2, 0.8, 0.5, 0.3],
            "XUTM32EUREF89": [725000.0, 726000.0, 727000.0, 725500.0, 726500.0],
            "YUTM32EUREF89": [6175000.0, 6176000.0, 6177000.0, 6175500.0, 6176500.0],
            "DGUNR": ["123.456", "234.567", "345.678", "456.789", "567.890"],
            "BORID": [100275, 100276, 100277, 100278, 100279],
            "INDTAGSID": ["1", "1", "2", "1", "1"],
            "BORINGSDYBDE": [59.0, 45.0, 72.0, 38.0, 55.0],
            "TERRAENKOTE": [84.59, 72.3, 91.2, 65.8, 78.4],
            "INDTAG_TOP": [48.0, 35.0, 60.0, 28.0, 45.0],
            "INDTAG_BUND": [57.0, 43.0, 70.0, 36.0, 53.0],
            "Grundvandsforekomst": [
                "DK111_dkmj_1090_ks",
                "DK111_dkmj_1091_ks",
                "DK111_dkmj_1092_ks",
                "DK111_dkmj_1093_ks",
                "DK111_dkmj_1094_ks",
            ],
            "REDOXVANDTYPE": ["C", "A", "B", "C", "A"],
            "Depth": ["Unknown", "Shallow", "Deep", "Unknown", "Shallow"],
            "DATATYPE": ["GRUMO", "GRUMO", "VF", "DEPOT", "GRUMO"],
            "euProgrammeCode": ["DKGRUMO", "DKGRUMO", "DKVandforsyning", "DKForurening", "DKGRUMO"],
            "euMonitoringSiteCode": [
                "DK115-1106-1",
                "DK115-1107-1",
                "DK115-1108-2",
                "DK115-1109-1",
                "DK115-1110-1",
            ],
            "ENHED": ["ng/L", "ng/L", "ng/L", "ng/L", "ng/L"],
        }
    )


def test_config_defaults() -> None:
    """Test default configuration values."""
    config = GEUSDataversePesticidesSilverConfig()

    assert config.dataset == "geus_dataverse_pesticides"
    assert config.bucket == "landbrugsdata-raw-data"
    assert config.source_crs == "EPSG:25832"


def test_duckdb_spatial_setup(
    silver_source: GEUSDataversePesticidesSilver,
) -> None:
    """Test that DuckDB spatial extension is loaded."""
    # Verify spatial extension is available by running a spatial query
    result = silver_source.conn.execute("SELECT ST_AsText(ST_Point(725000, 6175000))").fetchone()
    assert result is not None
    assert "POINT" in result[0]


def test_convert_rds_to_dataframe(
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test converting .rds file to DataFrame."""
    import sys
    from unittest.mock import MagicMock

    # Mock pyreadr module
    mock_pyreadr = MagicMock()
    mock_pyreadr.read_r.return_value = {None: sample_dataframe}

    # Inject mock into sys.modules before import
    sys.modules["pyreadr"] = mock_pyreadr

    try:
        result = silver_source._convert_rds_to_dataframe(Path("/tmp/test.rds"))

        assert len(result) == 5
        assert list(result.columns) == list(sample_dataframe.columns)
        mock_pyreadr.read_r.assert_called_once_with("/tmp/test.rds")
    finally:
        # Clean up
        del sys.modules["pyreadr"]


def test_transform_data(
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test transforming DataFrame to DuckDB table."""
    table_name = silver_source._transform_data(sample_dataframe)

    assert table_name == "pesticides_transformed"

    # Verify table exists and has correct columns
    columns = silver_source.conn.execute(f"DESCRIBE {table_name}").fetchall()
    column_names = {row[0] for row in columns}

    expected_columns = {
        "year",
        "stofnr",
        "stof_tekst",
        "cas_number",
        "maengde",
        "enhed",
        "x",
        "y",
        "dgu_nr",
        "borid",
        "indtagsid",
        "borehole_depth_m",
        "terrain_elevation_m",
        "intake_top_m",
        "intake_bottom_m",
        "groundwater_body",
        "redox_water_type",
        "depth_category",
        "data_type",
        "eu_programme_code",
        "eu_monitoring_site_code",
        "geometry",
    }
    assert expected_columns.issubset(column_names)

    # Verify row count
    count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    assert count == 5


def test_transform_data_statistics(
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test that transformation produces correct statistics."""
    table_name = silver_source._transform_data(sample_dataframe)

    # Check year range
    year_stats = silver_source.conn.execute(f"""
        SELECT MIN(year), MAX(year), COUNT(DISTINCT year)
        FROM {table_name}
    """).fetchone()

    assert year_stats[0] == 2020  # min year
    assert year_stats[1] == 2022  # max year
    assert year_stats[2] == 3  # unique years

    # Check substance count
    substance_count = silver_source.conn.execute(f"""
        SELECT COUNT(DISTINCT stofnr) FROM {table_name}
    """).fetchone()[0]
    assert substance_count == 4  # 438, 846, 1448, 2251


def test_validate_geometries_within_bounds(
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test geometry validation for points within Denmark bounds."""
    table_name = silver_source._transform_data(sample_dataframe)

    # All sample points are within Denmark bounds (EPSG:25832)
    silver_source._validate_geometries(table_name)

    # Should not raise any errors and log success
    silver_source.log.info.assert_called()


def test_validate_geometries_outside_bounds(
    silver_source: GEUSDataversePesticidesSilver,
) -> None:
    """Test geometry validation for points outside Denmark bounds."""
    # Create DataFrame with points outside Denmark (includes all required columns)
    df_outside = pd.DataFrame(
        {
            "PROEVEAAR": [2020.0],
            "STOFKODE": [438.0],
            "STOFNAVN": ["Test"],
            "AM": [0.1],
            "XUTM32EUREF89": [100000.0],  # Outside Denmark
            "YUTM32EUREF89": [5000000.0],  # Outside Denmark
            "DGUNR": ["123.456"],
            "BORID": [100275],
            "INDTAGSID": ["1"],
            "BORINGSDYBDE": [59.0],
            "TERRAENKOTE": [84.59],
            "INDTAG_TOP": [48.0],
            "INDTAG_BUND": [57.0],
            "Grundvandsforekomst": ["DK111_test"],
            "REDOXVANDTYPE": ["C"],
            "Depth": ["Unknown"],
            "DATATYPE": ["GRUMO"],
            "euProgrammeCode": ["DKGRUMO"],
            "euMonitoringSiteCode": ["DK115-1106-1"],
            "CASNUMBER": ["2008-58-4"],
            "ENHED": ["20"],
        }
    )

    table_name = silver_source._transform_data(df_outside)
    silver_source._validate_geometries(table_name)

    # Should log warning about points outside bounds
    silver_source.log.warning.assert_called()


@pytest.mark.asyncio
@patch(
    "unified_pipeline.silver.geus_dataverse_pesticides.GEUSDataversePesticidesSilver._download_rds_from_gcs"
)
@patch(
    "unified_pipeline.silver.geus_dataverse_pesticides.GEUSDataversePesticidesSilver._convert_rds_to_dataframe"
)
async def test_run_with_bronze_data(
    mock_convert: MagicMock,
    mock_download: MagicMock,
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test running silver processing with bronze manifest data."""
    mock_download.return_value = Path("/tmp/test.rds")
    mock_convert.return_value = sample_dataframe
    silver_source._save_data_with_metadata = MagicMock()

    bronze_manifest = {
        "dataset": "geus_dataverse_pesticides",
        "bucket": "test-bucket",
        "rds_path": "bronze/geus_dataverse_pesticides/2024-01-15/AM_pest.rds",
    }

    result = await silver_source.run(bronze_data=bronze_manifest)

    assert result is not None
    assert result["dataset"] == "test_geus_dataverse_pesticides"
    assert result["status"] == "completed"
    assert "statistics" in result
    # Stats are nested under statistics["pesticides"]
    assert result["statistics"]["pesticides"]["total_records"] == 5
    assert result["statistics"]["pesticides"]["unique_substances"] == 4
    # Backwards compatible flat fields
    assert result["total_records"] == 5
    assert result["unique_substances"] == 4

    mock_download.assert_called_once_with(bronze_manifest["rds_path"])
    mock_convert.assert_called_once()
    silver_source._save_data_with_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_run_missing_rds_path(
    silver_source: GEUSDataversePesticidesSilver,
) -> None:
    """Test running silver processing with missing rds_path in manifest."""
    bronze_manifest = {
        "dataset": "geus_dataverse_pesticides",
        "bucket": "test-bucket",
        # Missing rds_path
    }

    result = await silver_source.run(bronze_data=bronze_manifest)

    assert result is None
    silver_source.log.error.assert_called()


@pytest.mark.asyncio
async def test_run_no_manifest_found(
    silver_source: GEUSDataversePesticidesSilver,
) -> None:
    """Test running silver processing when no manifest is found in GCS."""
    # Mock GCS access methods to return empty list (no manifests)
    silver_source.gcs_access = MagicMock()
    silver_source.gcs_access.list_files.return_value = []

    result = await silver_source.run(bronze_data=None)

    # Should return None and log error
    assert result is None
    silver_source.log.error.assert_called()


def test_geometry_creation(
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
) -> None:
    """Test that geometries are created correctly."""
    table_name = silver_source._transform_data(sample_dataframe)

    # Check geometry type
    geom_check = silver_source.conn.execute(f"""
        SELECT ST_GeometryType(geometry) as geom_type
        FROM {table_name}
        LIMIT 1
    """).fetchone()

    assert geom_check[0] == "POINT"

    # Check coordinates match input
    coords = silver_source.conn.execute(f"""
        SELECT x, y, ST_X(geometry) as geom_x, ST_Y(geometry) as geom_y
        FROM {table_name}
        ORDER BY x
        LIMIT 1
    """).fetchone()

    assert coords[0] == coords[2]  # x == geom_x
    assert coords[1] == coords[3]  # y == geom_y


# ============================================================================
# PFAS-specific tests
# ============================================================================


def test_transform_pfas_data(
    silver_source: GEUSDataversePesticidesSilver,
    sample_pfas_dataframe: pd.DataFrame,
) -> None:
    """Test transforming PFAS DataFrame to DuckDB table."""
    table_name = silver_source._transform_pfas_data(sample_pfas_dataframe)

    assert table_name == "pfas_transformed"

    # Verify table exists and has correct columns
    columns = silver_source.conn.execute(f"DESCRIBE {table_name}").fetchall()
    column_names = {row[0] for row in columns}

    expected_columns = {
        "year",
        "substance_name",
        "substance_group",
        "maengde",
        "enhed",
        "x",
        "y",
        "dgu_nr",
        "borid",
        "indtagsid",
        "borehole_depth_m",
        "terrain_elevation_m",
        "intake_top_m",
        "intake_bottom_m",
        "groundwater_body",
        "redox_water_type",
        "depth_category",
        "data_type",
        "eu_programme_code",
        "eu_monitoring_site_code",
        "geometry",
    }
    assert expected_columns.issubset(column_names)

    # Verify row count
    count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    assert count == 5


def test_transform_pfas_data_statistics(
    silver_source: GEUSDataversePesticidesSilver,
    sample_pfas_dataframe: pd.DataFrame,
) -> None:
    """Test that PFAS transformation produces correct statistics."""
    table_name = silver_source._transform_pfas_data(sample_pfas_dataframe)

    # Check year range
    year_stats = silver_source.conn.execute(f"""
        SELECT MIN(year), MAX(year), COUNT(DISTINCT year)
        FROM {table_name}
    """).fetchone()

    assert year_stats[0] == 2020  # min year
    assert year_stats[1] == 2022  # max year
    assert year_stats[2] == 3  # unique years

    # Check substance count
    substance_count = silver_source.conn.execute(f"""
        SELECT COUNT(DISTINCT substance_name) FROM {table_name}
    """).fetchone()[0]
    assert substance_count == 5  # PFOS, PFOA, PFNA, PFHxS, PFBS

    # Check substance groups
    groups = silver_source.conn.execute(f"""
        SELECT DISTINCT substance_group FROM {table_name} ORDER BY substance_group
    """).fetchall()
    group_names = [row[0] for row in groups]
    assert "Perfluoroalkyl sulfonates" in group_names
    assert "Perfluorocarboxylic acids" in group_names


def test_pfas_geometry_creation(
    silver_source: GEUSDataversePesticidesSilver,
    sample_pfas_dataframe: pd.DataFrame,
) -> None:
    """Test that PFAS geometries are created correctly."""
    table_name = silver_source._transform_pfas_data(sample_pfas_dataframe)

    # Check geometry type
    geom_check = silver_source.conn.execute(f"""
        SELECT ST_GeometryType(geometry) as geom_type
        FROM {table_name}
        LIMIT 1
    """).fetchone()

    assert geom_check[0] == "POINT"

    # Check coordinates match input
    coords = silver_source.conn.execute(f"""
        SELECT x, y, ST_X(geometry) as geom_x, ST_Y(geometry) as geom_y
        FROM {table_name}
        ORDER BY x
        LIMIT 1
    """).fetchone()

    assert coords[0] == coords[2]  # x == geom_x
    assert coords[1] == coords[3]  # y == geom_y


@pytest.mark.asyncio
@patch(
    "unified_pipeline.silver.geus_dataverse_pesticides.GEUSDataversePesticidesSilver._download_rds_from_gcs"
)
@patch(
    "unified_pipeline.silver.geus_dataverse_pesticides.GEUSDataversePesticidesSilver._convert_rds_to_dataframe"
)
async def test_run_with_both_pest_and_pfas(
    mock_convert: MagicMock,
    mock_download: MagicMock,
    silver_source: GEUSDataversePesticidesSilver,
    sample_dataframe: pd.DataFrame,
    sample_pfas_dataframe: pd.DataFrame,
) -> None:
    """Test running silver processing with both pesticide and PFAS data."""
    mock_download.return_value = Path("/tmp/test.rds")
    # Return pest data first, then PFAS data
    mock_convert.side_effect = [sample_dataframe, sample_pfas_dataframe]
    silver_source._save_data_with_metadata = MagicMock()

    bronze_manifest = {
        "dataset": "geus_dataverse_pesticides",
        "bucket": "test-bucket",
        "rds_path": "bronze/geus_dataverse_pesticides/2024-01-15/AM_pest.rds",
        "pest_rds_path": "bronze/geus_dataverse_pesticides/2024-01-15/AM_pest.rds",
        "pfas_rds_path": "bronze/geus_dataverse_pesticides/2024-01-15/AM_pfas.rds",
    }

    result = await silver_source.run(bronze_data=bronze_manifest)

    assert result is not None
    assert result["dataset"] == "test_geus_dataverse_pesticides"
    assert result["status"] == "completed"
    assert "statistics" in result

    # Pesticide stats nested under statistics["pesticides"]
    assert result["statistics"]["pesticides"]["total_records"] == 5
    assert result["statistics"]["pesticides"]["unique_substances"] == 4
    # Backwards compatible flat fields
    assert result["total_records"] == 5
    assert result["unique_substances"] == 4

    # PFAS stats nested under statistics["pfas"]
    assert "pfas" in result["statistics"]
    assert result["statistics"]["pfas"]["total_records"] == 5
    assert result["statistics"]["pfas"]["unique_substances"] == 5

    # Should have downloaded both files
    assert mock_download.call_count == 2
    assert mock_convert.call_count == 2
    # Save should be called twice (pest + pfas)
    assert silver_source._save_data_with_metadata.call_count == 2
