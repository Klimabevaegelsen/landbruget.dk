"""
Tests for the WetlandsSilver class.
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, call, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from unified_pipeline.silver.wetlands import WetlandsSilver, WetlandsSilverConfig
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Return a mock GCSUtil instance."""
    mock_gcs = MagicMock(spec=GCSUtil)
    mock_gcs.read_parquet = MagicMock()
    mock_gcs.upload_blob = MagicMock()
    return mock_gcs


@pytest.fixture
def config() -> WetlandsSilverConfig:
    """Return a test configuration."""
    return WetlandsSilverConfig(
        dataset="test_wetlands",
        bucket="test-bucket",
        storage_batch_size=1000,
        namespaces={
            "wfs": "http://www.opengis.net/wfs/2.0",
            "natur": "http://wfs2-miljoegis.mim.dk/natur",
            "gml": "http://www.opengis.net/gml/3.2",
        },
        gml_ns="{http://www.opengis.net/gml/3.2}",
    )


@pytest.fixture
def silver_source(config: WetlandsSilverConfig, mock_gcs_util: MagicMock) -> WetlandsSilver:
    """Return a test WetlandsSilver instance."""
    source = WetlandsSilver(config, mock_gcs_util)
    source.log = MagicMock()
    return source


@pytest.fixture
def sample_xml() -> str:
    """Return a sample XML string for testing."""
    return """
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" 
                          xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" 
                          xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:kulstof2022 gml:id="id1">
            <natur:gridcode>1</natur:gridcode>
            <natur:toerv_pct>25</natur:toerv_pct>
            <gml:Polygon>
                <gml:exterior>
                    <gml:LinearRing>
                        <gml:posList>10.0 55.0 10.1 55.0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                    </gml:LinearRing>
                </gml:exterior>
            </gml:Polygon>
        </natur:kulstof2022>
        <natur:kulstof2022 gml:id="id2">
            <natur:gridcode>2</natur:gridcode>
            <natur:toerv_pct>35</natur:toerv_pct>
            <gml:Polygon>
                <gml:exterior>
                    <gml:LinearRing>
                        <gml:posList>11.0 56.0 11.1 56.0 11.1 56.1 11.0 56.1 11.0 56.0</gml:posList>
                    </gml:LinearRing>
                </gml:exterior>
            </gml:Polygon>
        </natur:kulstof2022>
    </wfs:FeatureCollection>
    """


@pytest.fixture
def sample_dataframe(sample_xml: str) -> pd.DataFrame:
    """Return a sample DataFrame with XML payloads."""
    return pd.DataFrame({"payload": [sample_xml]})


@pytest.fixture
def simple_geodataframe() -> gpd.GeoDataFrame:
    """Return a simple GeoDataFrame for testing the dissolve function."""
    data = {
        "id": ["1", "2", "3", "4"],
        "gridcode": [1, 1, 2, 2],
        "toerv_pct": ["25", "25", "35", "35"],
        "geometry": [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),  # Shares edge with polygon 2
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),  # Shares edge with polygon 1
            Polygon([(0, 2), (1, 2), (1, 3), (0, 3)]),  # Shares edge with polygon 4
            Polygon([(1, 2), (2, 2), (2, 3), (1, 3)]),  # Shares edge with polygon 3
        ],
    }
    return gpd.GeoDataFrame(data, crs="EPSG:25832")


def test_analyze_geometry(silver_source: WetlandsSilver) -> None:
    """Test analyzing a geometry."""
    geom = Polygon([(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)])
    result = silver_source.analyze_geometry(geom)

    assert result["width"] == 100
    assert result["height"] == 100
    assert result["area"] == 10000
    assert result["vertices"] == 5

    # Test grid alignment for a polygon that is aligned to a 10-unit grid
    grid_aligned_geom = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    result = silver_source.analyze_geometry(grid_aligned_geom)
    assert result["grid_aligned"] is True

    # Test grid alignment for a polygon that is not aligned to a 10-unit grid
    non_grid_aligned_geom = Polygon([(0, 0), (10.5, 0), (10.5, 10.5), (0, 10.5), (0, 0)])
    result = silver_source.analyze_geometry(non_grid_aligned_geom)
    assert result["grid_aligned"] is False


def test_log_geometry_statistics(silver_source: WetlandsSilver) -> None:
    """Test logging geometry statistics."""
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [
                Polygon([(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)]),
                Polygon([(200, 200), (300, 200), (300, 300), (200, 300), (200, 200)]),
            ]
        }
    )

    # Execute and verify no exceptions occur
    try:
        silver_source.log_geometry_statistics(gdf)
        # If we reach here, no exception was thrown
        exception_raised = False
    except Exception:
        exception_raised = True

    assert not exception_raised


def test_parse_geometry_valid(silver_source: WetlandsSilver) -> None:
    """Test parsing a valid geometry."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>10.0 55.0 10.1 55.0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </feature>
    """
    root = ET.fromstring(xml_str)
    geom_elem = root.find(".//gml:Polygon", silver_source.config.namespaces)

    if geom_elem is None:
        raise ValueError("Geometry element not found in XML")
    
    result = silver_source._parse_geometry(geom_elem)

    assert result is not None
    assert isinstance(result, Polygon)
    assert result.is_valid


def test_parse_geometry_invalid(silver_source: WetlandsSilver) -> None:
    """Test parsing an invalid geometry (missing posList)."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <Feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </Feature>
    """
    root = ET.fromstring(xml_str)
    geom_elem = root.find(".//gml:Polygon", silver_source.config.namespaces)

    if geom_elem is None:
        raise ValueError("Geometry element not found in XML")
    
    result = silver_source._parse_geometry(geom_elem)

    assert result is None


def test_get_attribute(silver_source: WetlandsSilver) -> None:
    """Test getting an attribute from an XML element."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <feature xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:gridcode>1</natur:gridcode>
        <natur:toerv_pct>25</natur:toerv_pct>
    </feature>
    """
    root = ET.fromstring(xml_str)

    gridcode = silver_source._get_attribute(root, "natur:gridcode")
    toerv_pct = silver_source._get_attribute(root, "natur:toerv_pct")
    missing = silver_source._get_attribute(root, "natur:missing")

    assert gridcode == "1"
    assert toerv_pct == "25"
    assert missing is None


def test_parse_feature_valid(silver_source: WetlandsSilver) -> None:
    """Test parsing a valid feature."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <natur:kulstof2022 gml:id="id1" xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:gridcode>1</natur:gridcode>
        <natur:toerv_pct>25</natur:toerv_pct>
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>10.0 55.0 10.1 55.0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </natur:kulstof2022>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_feature(root)

    assert result is not None
    assert result["type"] == "Feature"
    assert result["properties"]["id"] == "id1"
    assert result["properties"]["gridcode"] == 1
    assert result["properties"]["toerv_pct"] == "25"
    assert result["geometry"] is not None


def test_parse_feature_missing_geometry(silver_source: WetlandsSilver) -> None:
    """Test parsing a feature with missing geometry."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <natur:kulstof2022 gml:id="id1" xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:gridcode>1</natur:gridcode>
        <natur:toerv_pct>25</natur:toerv_pct>
    </natur:kulstof2022>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_feature(root)

    assert result is None


def test_parse_feature_missing_gridcode(silver_source: WetlandsSilver) -> None:
    """Test parsing a feature with missing gridcode."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <natur:kulstof2022 gml:id="id1" xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:toerv_pct>25</natur:toerv_pct>
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>10.0 55.0 10.1 55.0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </natur:kulstof2022>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_feature(root)

    assert result is None


def test_parse_feature_missing_toerv_pct(silver_source: WetlandsSilver) -> None:
    """Test parsing a feature with missing toerv_pct."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <natur:kulstof2022 gml:id="id1" xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:gridcode>1</natur:gridcode>
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>10.0 55.0 10.1 55.0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </natur:kulstof2022>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_feature(root)

    assert result is None


def test_process_xml_data_empty(silver_source: WetlandsSilver) -> None:
    """Test processing empty XML data."""
    result = silver_source._process_xml_data(pd.DataFrame())
    assert result is None


@patch("unified_pipeline.silver.wetlands.ET.fromstring")
def test_process_xml_data_error(mock_fromstring: MagicMock, silver_source: WetlandsSilver) -> None:
    """Test error handling when processing XML data."""
    mock_fromstring.side_effect = Exception("Test error")

    with pytest.raises(Exception):
        silver_source._process_xml_data(pd.DataFrame({"payload": ["<invalid>"]}))


def test_process_xml_data_success(
    silver_source: WetlandsSilver, sample_dataframe: pd.DataFrame
) -> None:
    """Test successfully processing XML data."""
    result = silver_source._process_xml_data(sample_dataframe)

    assert result is not None
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 2
    assert "id" in result.columns
    assert "gridcode" in result.columns
    assert "toerv_pct" in result.columns
    assert result.crs == "EPSG:25832"


@patch("unified_pipeline.silver.wetlands.validate_and_transform_geometries")
def test_create_dissolved_df(
    mock_validate: MagicMock, silver_source: WetlandsSilver, simple_geodataframe: gpd.GeoDataFrame
) -> None:
    """Test creating dissolved dataframe."""
    # Create mock result with expected dissolution - two features from four
    dissolved = simple_geodataframe.copy()
    dissolved["wetland_id"] = dissolved["gridcode"]
    dissolved = dissolved.dissolve(by="gridcode", as_index=True).reset_index()
    dissolved["wetland_id"] = [1, 2]

    # Set the mock to return our prepared dissolved dataframe
    mock_validate.return_value = dissolved

    result = silver_source._create_dissolved_df(simple_geodataframe, "test")

    # Should have 2 features after dissolving (polygons 1+2 and 3+4)
    assert len(result) == 2
    assert "wetland_id" in result.columns
    assert result["wetland_id"].tolist() == [1, 2]

    # Check validation call - use any_call instead of directly comparing DataFrames
    mock_validate.assert_called_once()
    args, kwargs = mock_validate.call_args
    assert args[1] == "silver.test_dissolved"


@patch("unified_pipeline.silver.wetlands.validate_and_transform_geometries")
def test_create_dissolved_df_error(
    mock_validate: MagicMock, silver_source: WetlandsSilver, simple_geodataframe: gpd.GeoDataFrame
) -> None:
    """Test error handling in create_dissolved_df."""
    mock_validate.side_effect = Exception("Test error")

    with pytest.raises(Exception):
        silver_source._create_dissolved_df(simple_geodataframe, "test")


@pytest.mark.asyncio
async def test_run_success(silver_source: WetlandsSilver) -> None:
    """Test successful run of the pipeline."""
    # Mock methods
    silver_source._read_bronze_data = MagicMock(
        return_value=pd.DataFrame({"payload": ["<xml></xml>"]})
    )
    silver_source._process_xml_data = MagicMock(return_value=gpd.GeoDataFrame())
    silver_source._create_dissolved_df = MagicMock(return_value=gpd.GeoDataFrame())
    silver_source._save_data = MagicMock()

    await silver_source.run()

    # Verify method calls
    silver_source._read_bronze_data.assert_called_once_with(
        silver_source.config.dataset, silver_source.config.bucket
    )
    silver_source._process_xml_data.assert_called_once()
    silver_source._create_dissolved_df.assert_called_once()
    assert silver_source._save_data.call_count == 2
    silver_source._save_data.assert_has_calls(
        [
            call(
                silver_source._process_xml_data.return_value,
                silver_source.config.dataset,
                silver_source.config.bucket,
            ),
            call(
                silver_source._create_dissolved_df.return_value,
                f"{silver_source.config.dataset}_dissolved",
                silver_source.config.bucket,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_run_read_bronze_data_error(silver_source: WetlandsSilver) -> None:
    """Test run with error in reading bronze data."""
    silver_source._read_bronze_data = MagicMock(return_value=None)
    silver_source._process_xml_data = MagicMock()
    silver_source._create_dissolved_df = MagicMock()
    silver_source._save_data = MagicMock()

    await silver_source.run()

    assert not silver_source._process_xml_data.called
    assert not silver_source._create_dissolved_df.called


@pytest.mark.asyncio
async def test_run_process_xml_data_error(silver_source: WetlandsSilver) -> None:
    """Test run with error in processing XML data."""
    silver_source._read_bronze_data = MagicMock(return_value=pd.DataFrame())
    silver_source._process_xml_data = MagicMock(return_value=None)
    silver_source._create_dissolved_df = MagicMock()
    silver_source._save_data = MagicMock()

    await silver_source.run()

    assert not silver_source._create_dissolved_df.called
    assert not silver_source._save_data.called

    assert not silver_source._create_dissolved_df.called
    assert not silver_source._save_data.called
