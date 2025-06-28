"""
Tests for FVM WFS Silver layer.
"""

import json
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from unified_pipeline.silver.fvm_wfs import FVMWFSSilver, FVMWFSSilverConfig


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Create a mock GCS utility."""
    mock = MagicMock()
    mock.upload_blob = MagicMock()
    mock.download_blob = MagicMock()
    return mock


@pytest.fixture
def config() -> FVMWFSSilverConfig:
    """Create a test configuration."""
    return FVMWFSSilverConfig(save_local=True)


@pytest.fixture
def fvm_wfs_silver(config: FVMWFSSilverConfig, mock_gcs_util: MagicMock) -> FVMWFSSilver:
    """Create a FVM WFS silver instance."""
    return FVMWFSSilver(config, mock_gcs_util)


@pytest.fixture
def sample_bronze_data() -> pd.DataFrame:
    """Create sample bronze data for testing."""
    # Sample GeoJSON feature
    feature = {
        "type": "Feature",
        "properties": {"id": "123", "area": 1500.0, "crop_type": "wheat", "year": 2024},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[10.0, 55.0], [10.1, 55.0], [10.1, 55.1], [10.0, 55.1], [10.0, 55.0]]],
        },
    }

    feature_collection = {"type": "FeatureCollection", "features": [feature]}

    return pd.DataFrame(
        [
            {
                "payload": json.dumps(feature_collection),
                "source": "Danish FVM WFS Agricultural Data",
                "layer_type": "Markblokke",
                "year": 2024,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
        ]
    )


def test_fvm_wfs_silver_config() -> None:
    """Test FVM WFS silver configuration."""
    config = FVMWFSSilverConfig()

    assert config.name == "Danish FVM WFS Agricultural Data - Silver"
    assert config.type == "transformation"
    assert config.dataset_markblokke == "fvm_markblokke_silver"
    assert config.dataset_marker == "fvm_marker_silver"
    assert config.dataset_smaabiotoper == "fvm_smaabiotoper_silver"


def test_parse_geojson_valid(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test parsing valid GeoJSON."""
    geojson_str = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": "123", "name": "test"},
                    "geometry": {"type": "Point", "coordinates": [10.0, 55.0]},
                }
            ],
        }
    )

    result = fvm_wfs_silver._parse_geojson(geojson_str)

    assert result is not None
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 1
    assert result["features"][0]["properties"]["id"] == "123"


def test_parse_geojson_invalid(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test parsing invalid GeoJSON."""
    invalid_json = "not valid json"

    result = fvm_wfs_silver._parse_geojson(invalid_json)
    assert result is None


def test_parse_geojson_empty_features(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test parsing GeoJSON with empty features."""
    geojson_str = json.dumps({"type": "FeatureCollection", "features": []})

    result = fvm_wfs_silver._parse_geojson(geojson_str)
    assert result is None


def test_extract_features_from_row(
    fvm_wfs_silver: FVMWFSSilver, sample_bronze_data: pd.DataFrame
) -> None:
    """Test feature extraction from bronze data row."""
    row = sample_bronze_data.iloc[0]

    features = fvm_wfs_silver._extract_features_from_row(row)

    assert len(features) == 1
    feature = features[0]

    # Check that metadata is added
    assert feature["properties"]["source"] == "Danish FVM WFS Agricultural Data"
    assert feature["properties"]["layer_type"] == "Markblokke"
    assert feature["properties"]["year"] == 2024
    assert "processed_at" in feature["properties"]

    # Check original properties are preserved
    assert feature["properties"]["id"] == "123"
    assert feature["properties"]["area"] == 1500.0
    assert feature["properties"]["crop_type"] == "wheat"


def test_convert_to_geodataframe(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test conversion of features to GeoDataFrame."""
    features = [
        {
            "type": "Feature",
            "properties": {"id": "123", "area": 1500.0, "layer_type": "Markblokke", "year": 2024},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[10.0, 55.0], [10.1, 55.0], [10.1, 55.1], [10.0, 55.1], [10.0, 55.0]]
                ],
            },
        }
    ]

    gdf = fvm_wfs_silver._convert_to_geodataframe(features)

    assert len(gdf) == 1
    assert "geometry" in gdf.columns
    assert gdf["id"].iloc[0] == "123"
    assert gdf["area"].iloc[0] == 1500.0
    assert gdf["layer_type"].iloc[0] == "Markblokke"
    assert gdf["year"].iloc[0] == 2024

    # Check geometry is properly converted
    assert gdf.geometry.iloc[0].geom_type == "Polygon"


def test_convert_to_geodataframe_empty(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test conversion of empty features list."""
    gdf = fvm_wfs_silver._convert_to_geodataframe([])

    assert len(gdf) == 0
    assert "geometry" in gdf.columns


def test_validate_geometry_valid(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test geometry validation with valid geometries."""
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame({"id": ["1", "2"], "geometry": [Point(10, 55), Point(11, 56)]})

    result = fvm_wfs_silver._validate_geometry(gdf)

    assert len(result) == 2
    assert all(result.geometry.is_valid)


def test_validate_geometry_invalid(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test geometry validation with invalid geometries."""
    import geopandas as gpd
    from shapely.geometry import Polygon

    # Create invalid polygon (self-intersecting)
    invalid_polygon = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    valid_polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    gdf = gpd.GeoDataFrame({"id": ["1", "2"], "geometry": [invalid_polygon, valid_polygon]})

    result = fvm_wfs_silver._validate_geometry(gdf)

    # Should only keep valid geometries
    assert len(result) == 1
    assert result["id"].iloc[0] == "2"


def test_get_dataset_name(fvm_wfs_silver: FVMWFSSilver) -> None:
    """Test dataset name generation."""
    assert fvm_wfs_silver._get_dataset_name("Markblokke") == "fvm_markblokke_silver"
    assert fvm_wfs_silver._get_dataset_name("Marker") == "fvm_marker_silver"
    assert fvm_wfs_silver._get_dataset_name("Smaabiotoper") == "fvm_smaabiotoper_silver"
    assert fvm_wfs_silver._get_dataset_name("Unknown") == "fvm_unknown_silver"


def test_create_dataframe(fvm_wfs_silver: FVMWFSSilver, sample_bronze_data: pd.DataFrame) -> None:
    """Test DataFrame creation from bronze data."""
    result = fvm_wfs_silver.create_dataframe(sample_bronze_data)

    # Should have separate DataFrames for each layer type
    assert "Markblokke" in result

    markblokke_df = result["Markblokke"]
    assert len(markblokke_df) == 1
    assert "geometry" in markblokke_df.columns
    assert markblokke_df["id"].iloc[0] == "123"
    assert markblokke_df["layer_type"].iloc[0] == "Markblokke"
