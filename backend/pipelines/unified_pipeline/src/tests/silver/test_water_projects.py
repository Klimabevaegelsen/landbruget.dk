"""
Tests for the WaterProjectsSilver class.
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from unittest.mock import MagicMock, PropertyMock, call, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, Polygon

from unified_pipeline.silver.water_projects import WaterProjectsSilver, WaterProjectsSilverConfig
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Return a mock GCSUtil instance."""
    mock_gcs = MagicMock(spec=GCSUtil)
    mock_gcs.read_parquet = MagicMock()
    mock_gcs.upload_blob = MagicMock()
    return mock_gcs


@pytest.fixture
def config() -> WaterProjectsSilverConfig:
    """Return a test configuration."""
    return WaterProjectsSilverConfig(
        dataset="test_water_projects",
        bucket="test-bucket",
        storage_batch_size=1000,
        namespaces={
            "wfs": "http://www.opengis.net/wfs/2.0",
            "natur": "http://wfs2-miljoegis.mim.dk/natur",
            "gml": "http://www.opengis.net/gml/3.2",
        },
        gml_ns="{http://www.opengis.net/gml/3.2}",
        layers=["test_layer1", "test_layer2"],
        service_types={"test_layer2": "arcgis"},
    )


@pytest.fixture
def silver_source(
    config: WaterProjectsSilverConfig, mock_gcs_util: MagicMock
) -> WaterProjectsSilver:
    """Return a test WaterProjectsSilver instance."""
    source = WaterProjectsSilver(config, mock_gcs_util)
    return source


@pytest.fixture
def sample_xml_root() -> ET.Element:
    """Return a sample XML root for testing."""
    xml_string = """
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" 
                         xmlns:gml="http://www.opengis.net/gml/3.2"
                         xmlns:test="http://test.namespace">
        <wfs:member>
            <test:Feature>
                <test:the_geom>
                    <gml:MultiSurface>
                        <gml:surfaceMember>
                            <gml:Polygon>
                                <gml:exterior>
                                    <gml:LinearRing>
                                        <gml:posList>
                                            10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                                        </gml:posList>
                                    </gml:LinearRing>
                                </gml:exterior>
                            </gml:Polygon>
                        </gml:surfaceMember>
                    </gml:MultiSurface>
                </test:the_geom>
                <test:id>123</test:id>
                <test:name>Test Feature</test:name>
                <test:area>100.5</test:area>
                <test:startaar>2020</test:startaar>
                <test:startdato>01-05-2020</test:startdato>
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """
    return ET.fromstring(xml_string)


@pytest.fixture
def sample_xml_string() -> str:
    """Return a sample XML string for testing."""
    return """
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" 
                         xmlns:gml="http://www.opengis.net/gml/3.2"
                         xmlns:test="http://test.namespace">
        <wfs:member>
            <test:Feature>
                <test:the_geom>
                    <gml:MultiSurface>
                        <gml:surfaceMember>
                            <gml:Polygon>
                                <gml:exterior>
                                    <gml:LinearRing>
                                        <gml:posList>
                                            10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                                        </gml:posList>
                                    </gml:LinearRing>
                                </gml:exterior>
                            </gml:Polygon>
                        </gml:surfaceMember>
                    </gml:MultiSurface>
                </test:the_geom>
                <test:id>123</test:id>
                <test:name>Test Feature</test:name>
                <test:area>100.5</test:area>
                <test:startaar>2020</test:startaar>
                <test:startdato>01-05-2020</test:startdato>
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """


@pytest.fixture
def sample_feature_element() -> ET.Element:
    """Return a sample feature element for testing."""
    xml_string = """
    <test:Feature xmlns:test="http://test.namespace" 
                xmlns:gml="http://www.opengis.net/gml/3.2">
        <test:the_geom>
            <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>
                                    10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
            </gml:MultiSurface>
        </test:the_geom>
        <test:id>123</test:id>
        <test:name>Test Feature</test:name>
        <test:area>100.5</test:area>
        <test:startaar>2020</test:startaar>
        <test:startdato>01-05-2020</test:startdato>
    </test:Feature>
    """
    return ET.fromstring(xml_string)


@pytest.fixture
def sample_geom_element() -> ET.Element:
    """Return a sample geometry element for testing."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    return ET.fromstring(xml_string)


@pytest.fixture
def sample_json_string() -> str:
    """Return a sample JSON string for testing."""
    return json.dumps(
        {
            "features": [
                {
                    "attributes": {
                        "projektnavn": "Test Project",
                        "enhedskontakt": "Test Contact",
                        "projektstart": 1577836800000,  # 2020-01-01 00:00:00
                        "projektslut": 1609459200000,  # 2021-01-01 00:00:00
                        "status": "Active",
                        "OBJECTID": 1,
                        "GlobalID": "abc123",
                    },
                    "geometry": {
                        "rings": [
                            [[10.0, 10.0], [20.0, 10.0], [20.0, 20.0], [10.0, 20.0], [10.0, 10.0]]
                        ]
                    },
                }
            ]
        }
    )


@pytest.fixture
def sample_bronze_df() -> pd.DataFrame:
    """Return a sample DataFrame with bronze data."""
    return pd.DataFrame(
        {
            "layer": ["test_layer1", "test_layer2"],
            "payload": [
                """
            <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" 
                                xmlns:gml="http://www.opengis.net/gml/3.2"
                                xmlns:test="http://test.namespace">
                <wfs:member>
                    <test:Feature>
                        <test:the_geom>
                            <gml:MultiSurface>
                                <gml:surfaceMember>
                                    <gml:Polygon>
                                        <gml:exterior>
                                            <gml:LinearRing>
                                                <gml:posList>
                                                    10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                                                </gml:posList>
                                            </gml:LinearRing>
                                        </gml:exterior>
                                    </gml:Polygon>
                                </gml:surfaceMember>
                            </gml:MultiSurface>
                        </test:the_geom>
                        <test:id>123</test:id>
                        <test:name>Test Feature</test:name>
                        <test:area>100.5</test:area>
                        <test:startaar>2020</test:startaar>
                        <test:startdato>01-05-2020</test:startdato>
                    </test:Feature>
                </wfs:member>
            </wfs:FeatureCollection>
            """,  # noqa: E501
                json.dumps(
                    {
                        "features": [
                            {
                                "attributes": {
                                    "projektnavn": "Test Project",
                                    "enhedskontakt": "Test Contact",
                                    "projektstart": 1577836800000,  # 2020-01-01 00:00:00
                                    "projektslut": 1609459200000,  # 2021-01-01 00:00:00
                                    "status": "Active",
                                    "OBJECTID": 1,
                                    "GlobalID": "abc123",
                                },
                                "geometry": {
                                    "rings": [
                                        [
                                            [10.0, 10.0],
                                            [20.0, 10.0],
                                            [20.0, 20.0],
                                            [10.0, 20.0],
                                            [10.0, 10.0],
                                        ]
                                    ]
                                },
                            }
                        ]
                    }
                ),
            ],
        }
    )


@pytest.fixture
def sample_geodataframe() -> gpd.GeoDataFrame:
    """Return a sample GeoDataFrame for testing."""
    polygon = Polygon([(10, 10), (20, 10), (20, 20), (10, 20)])
    return gpd.GeoDataFrame(
        {
            "layer": ["test_layer1", "test_layer2"],
            "area_ha": [1.0, 1.0],
            "name": ["Feature 1", "Feature 2"],
            "id": [1, 2],
        },
        geometry=[polygon, polygon],
        crs="EPSG:25832",
    )


def test_config_defaults() -> None:
    """Test that the config defaults are set correctly."""
    config = WaterProjectsSilverConfig()
    assert config.dataset == "water_projects"
    assert config.bucket == "landbrugsdata-raw-data"
    assert config.storage_batch_size == 5000
    assert config.namespaces["wfs"] == "http://www.opengis.net/wfs/2.0"
    assert config.namespaces["gml"] == "http://www.opengis.net/gml/3.2"
    assert config.gml_ns == "{http://www.opengis.net/gml/3.2}"
    assert len(config.layers) > 0
    assert "Klima_lavbund_demarkation___offentlige_projekter:0" in config.service_types
    assert config.service_types["Klima_lavbund_demarkation___offentlige_projekter:0"] == "arcgis"


def test_get_first_namespace_success(
    silver_source: WaterProjectsSilver, sample_xml_root: ET.Element
) -> None:
    """Test get_first_namespace successfully extracts namespace."""
    namespace = silver_source.get_first_namespace(sample_xml_root)
    assert namespace == "http://www.opengis.net/wfs/2.0"


def test_get_first_namespace_no_namespace(silver_source: WaterProjectsSilver) -> None:
    """Test get_first_namespace returns None when no namespace is found."""
    root = ET.fromstring("<root><child>Test</child></root>")
    namespace = silver_source.get_first_namespace(root)
    assert namespace is None


def test_clean_value_string(silver_source: WaterProjectsSilver) -> None:
    """Test clean_value with string values."""
    assert silver_source.clean_value("  Test  ") == "Test"
    assert silver_source.clean_value("") is None
    assert silver_source.clean_value("  ") is None


def test_clean_value_non_string(silver_source: WaterProjectsSilver) -> None:
    """Test clean_value with non-string values."""
    assert silver_source.clean_value(123) == "123"
    assert silver_source.clean_value(None) == "None"
    assert silver_source.clean_value(True) == "True"


def test_parse_geometry_success(
    silver_source: WaterProjectsSilver, sample_geom_element: ET.Element
) -> None:
    """Test _parse_geometry with valid geometry."""
    result = silver_source._parse_geometry(sample_geom_element)
    assert result is not None
    assert "wkt" in result
    assert "area_ha" in result
    assert "POLYGON" in result["wkt"]
    # Area is in square meters / 10000 for hectares, so 100 sq meters = 0.01 ha
    assert result["area_ha"] > 0


def test_parse_geometry_no_multisurface(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with missing MultiSurface."""
    geom_elem = ET.fromstring("<test:the_geom xmlns:test='http://test.namespace'></test:the_geom>")
    result = silver_source._parse_geometry(geom_elem)
    assert result is None


def test_parse_geometry_invalid_coordinates(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with invalid coordinates."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                invalid coordinates
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(xml_string)
    result = silver_source._parse_geometry(geom_elem)
    assert result is None


def test_parse_geometry_insufficient_coordinates(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with insufficient coordinates."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                10.0 10.0 20.0 20.0
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(xml_string)
    result = silver_source._parse_geometry(geom_elem)
    assert result is None


def test_parse_geometry_multiple_polygons(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with multiple polygons."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                30.0 30.0 40.0 30.0 40.0 40.0 30.0 40.0 30.0 30.0
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(xml_string)
    result = silver_source._parse_geometry(geom_elem)
    assert result is not None
    assert "MULTIPOLYGON" in result["wkt"]
    assert result["area_ha"] > 0


def test_parse_feature_success(
    silver_source: WaterProjectsSilver, sample_feature_element: ET.Element
) -> None:
    """Test _parse_feature with valid feature."""
    result = silver_source._parse_feature(sample_feature_element)
    assert result is not None
    assert "geometry" in result
    assert "area_ha" in result
    assert "id" in result
    assert "name" in result
    assert "area" in result
    assert result["id"] == "123"
    assert result["name"] == "Test Feature"
    assert result["area"] == 100.5
    assert result["startaar"] == 2020
    # Verify date parsing
    assert isinstance(result["startdato"], pd.Timestamp)


def test_parse_feature_no_geometry(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_feature with missing geometry."""
    xml_string = """
    <test:Feature xmlns:test="http://test.namespace">
        <test:id>123</test:id>
        <test:name>Test Feature</test:name>
    </test:Feature>
    """
    feature = ET.fromstring(xml_string)
    result = silver_source._parse_feature(feature)
    assert result is None


def test_parse_feature_invalid_geometry(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_feature with invalid geometry."""
    xml_string = """
    <test:Feature xmlns:test="http://test.namespace">
        <test:the_geom>
            <invalid>geometry</invalid>
        </test:the_geom>
        <test:id>123</test:id>
        <test:name>Test Feature</test:name>
    </test:Feature>
    """
    feature = ET.fromstring(xml_string)
    result = silver_source._parse_feature(feature)
    assert result is None


def test_parse_feature_conversion_errors(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_feature with values that can't be converted."""
    xml_string = """
    <test:Feature xmlns:test="http://test.namespace" 
                xmlns:gml="http://www.opengis.net/gml/3.2">
        <test:the_geom>
            <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>
                                    10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
            </gml:MultiSurface>
        </test:the_geom>
        <test:id>123</test:id>
        <test:area>not_a_number</test:area>
        <test:startaar>not_a_year</test:startaar>
        <test:startdato>not_a_date</test:startdato>
    </test:Feature>
    """
    feature = ET.fromstring(xml_string)
    result = silver_source._parse_feature(feature)
    assert result is not None
    assert "area" not in result or result["area"] is None
    assert "startaar" not in result or result["startaar"] is None
    assert "startdato" not in result or result["startdato"] is None


def test_process_xml_data_success(
    silver_source: WaterProjectsSilver, sample_xml_string: str
) -> None:
    """Test _process_xml_data with valid XML."""
    result = silver_source._process_xml_data(sample_xml_string, "test_layer")
    assert len(result) == 1
    assert "geometry" in result[0]
    assert "layer" in result[0]
    assert result[0]["layer"] == "test_layer"


def test_process_xml_data_no_namespace(silver_source: WaterProjectsSilver) -> None:
    """Test _process_xml_data with XML missing namespace."""
    xml_string = """
    <FeatureCollection>
        <member>
            <Feature>
                <id>123</id>
            </Feature>
        </member>
    </FeatureCollection>
    """
    with pytest.raises(Exception) as excinfo:
        silver_source._process_xml_data(xml_string, "test_layer")
    assert "No namespace found in XML" in str(excinfo.value)


def test_process_xml_data_no_features(silver_source: WaterProjectsSilver) -> None:
    """Test _process_xml_data with XML containing no valid features."""
    xml_string = """
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0">
        <wfs:member>
            <test:Feature xmlns:test="http://test.namespace">
                <test:id>123</test:id>
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """
    result = silver_source._process_xml_data(xml_string, "test_layer")
    assert len(result) == 0


def test_process_json_data_success(
    silver_source: WaterProjectsSilver, sample_json_string: str
) -> None:
    """Test _process_json_data with valid JSON."""
    result = silver_source._process_json_data(sample_json_string, "test_layer")
    assert len(result) == 1
    assert "geometry" in result[0]
    assert "layer_name" in result[0]
    assert result[0]["layer_name"] == "test_layer"
    assert "projektnavn" in result[0]
    assert "area_ha" in result[0]
    assert result[0]["area_ha"] > 0
    # Verify date parsing
    assert isinstance(result[0]["startdato"], datetime)
    assert isinstance(result[0]["slutdato"], datetime)


def test_process_json_data_missing_rings(silver_source: WaterProjectsSilver) -> None:
    """Test _process_json_data with JSON missing rings."""
    json_string = json.dumps(
        {
            "features": [
                {
                    "attributes": {"projektnavn": "Test Project"},
                    "geometry": {
                        "points": [10.0, 10.0]  # Not 'rings'
                    },
                }
            ]
        }
    )
    result = silver_source._process_json_data(json_string, "test_layer")
    assert len(result) == 0


def test_process_json_data_invalid_geometry(silver_source: WaterProjectsSilver) -> None:
    """Test _process_json_data with JSON containing invalid geometry."""
    json_string = json.dumps(
        {
            "features": [
                {
                    "attributes": {"projektnavn": "Test Project"},
                    "geometry": {
                        "rings": [
                            [
                                [10.0, 10.0],
                                [20.0, 20.0],  # Not enough points for a polygon
                            ]
                        ]
                    },
                }
            ]
        }
    )
    result = silver_source._process_json_data(json_string, "test_layer")
    assert len(result) == 0


def test_process_data_success(
    silver_source: WaterProjectsSilver, sample_bronze_df: pd.DataFrame
) -> None:
    """Test _process_data with valid bronze data."""
    with (
        patch.object(silver_source, "_process_xml_data") as mock_process_xml,
        patch.object(silver_source, "_process_json_data") as mock_process_json,
    ):
        # Setup mock return values
        mock_process_xml.return_value = [
            {
                "geometry": "POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))",
                "area_ha": 1.0,
                "id": "123",
                "layer": "test_layer1",
            }
        ]
        mock_process_json.return_value = [
            {
                "geometry": "POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))",
                "area_ha": 1.0,
                "projektnavn": "Test Project",
                "layer_name": "test_layer2",
            }
        ]

        result = silver_source._process_data(sample_bronze_df)

        # Verify result
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2
        assert result.crs == "EPSG:25832"

        # Verify method calls
        mock_process_xml.assert_called_once()
        mock_process_json.assert_called_once()


def test_process_data_empty_dataframe(silver_source: WaterProjectsSilver) -> None:
    """Test _process_data with empty DataFrame."""
    empty_df = pd.DataFrame()
    result = silver_source._process_data(empty_df)
    assert result is None


def test_process_data_no_features_extracted(
    silver_source: WaterProjectsSilver, sample_bronze_df: pd.DataFrame
) -> None:
    """Test _process_data when no features are extracted."""
    with (
        patch.object(silver_source, "_process_xml_data") as mock_process_xml,
        patch.object(silver_source, "_process_json_data") as mock_process_json,
    ):
        # Setup mock return values to return empty lists
        mock_process_xml.return_value = []
        mock_process_json.return_value = []

        result = silver_source._process_data(sample_bronze_df)

        # Verify result
        assert result is None


def test_process_data_processing_error(
    silver_source: WaterProjectsSilver, sample_bronze_df: pd.DataFrame
) -> None:
    """Test _process_data when processing raises an exception for XML data but JSON data still processes."""  # noqa: E501
    with patch.object(silver_source, "_process_xml_data") as mock_process_xml:
        # Setup mock to raise exception for XML data only
        mock_process_xml.side_effect = Exception("Processing error")

        # Process should continue for JSON data
        result = silver_source._process_data(sample_bronze_df)

        # We should still get a GeoDataFrame since the JSON data should still process
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1  # Only JSON data is processed


def test_create_dissolved_df_success(
    silver_source: WaterProjectsSilver, sample_geodataframe: gpd.GeoDataFrame
) -> None:
    """Test _create_dissolved_df with valid GeoDataFrame."""
    # Test directly without mocking validate_and_transform_geometries
    result = silver_source._create_dissolved_df(sample_geodataframe, "test_dataset")

    # Verify result
    assert result is not None
    assert isinstance(result, gpd.GeoDataFrame)
    assert result.crs == "EPSG:4326"  # Should be converted to WGS84


def test_create_dissolved_df_multipolygon(silver_source: WaterProjectsSilver) -> None:
    """Test _create_dissolved_df with MultiPolygon geometries."""
    # Create a GeoDataFrame with two separate polygons that won't dissolve into one
    polygon1 = Polygon([(10, 10), (20, 10), (20, 20), (10, 20)])
    polygon2 = Polygon([(30, 30), (40, 30), (40, 40), (30, 40)])
    gdf = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[polygon1, polygon2], crs="EPSG:25832")

    with patch(
        "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
    ) as mock_validate:
        # Setup mock to return a GeoDataFrame with the polygons
        mock_validate.return_value = gpd.GeoDataFrame(
            geometry=[polygon1, polygon2], crs="EPSG:4326"
        )

        result = silver_source._create_dissolved_df(gdf, "test_dataset")

        # Verify result
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2  # Should have 2 separate polygons


def test_create_dissolved_df_invalid_geometries(
    silver_source: WaterProjectsSilver, sample_geodataframe: gpd.GeoDataFrame
) -> None:
    """Test _create_dissolved_df with invalid geometries."""
    # Mock unary_union to return an invalid polygon
    with (
        patch("unified_pipeline.silver.water_projects.unary_union") as mock_unary_union,
        patch("unified_pipeline.silver.water_projects.explain_validity") as mock_explain_validity,
        patch(
            "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
        ) as mock_validate,
    ):
        # Create a MultiPolygon with invalid parts
        invalid_poly1 = Polygon([(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)])  # Self-intersecting
        invalid_poly2 = Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)])  # Valid
        invalid_multi = MultiPolygon([invalid_poly1, invalid_poly2])

        # Setup mocks
        mock_unary_union.return_value = invalid_multi
        mock_explain_validity.return_value = "Self-intersection at or near point 0.5 0.5"
        mock_validate.return_value = gpd.GeoDataFrame(
            geometry=[invalid_poly1.buffer(0), invalid_poly2],  # Buffer(0) fixes self-intersection
            crs="EPSG:4326",
        )

        result = silver_source._create_dissolved_df(sample_geodataframe, "test_dataset")

        # Verify result
        assert result is not None
        assert isinstance(result, gpd.GeoDataFrame)

        mock_explain_validity.assert_called()


def test_create_dissolved_df_exception(
    silver_source: WaterProjectsSilver, sample_geodataframe: gpd.GeoDataFrame
) -> None:
    """Test _create_dissolved_df when an exception occurs."""
    with patch("unified_pipeline.silver.water_projects.unary_union") as mock_unary_union:
        # Setup mock to raise exception
        mock_unary_union.side_effect = Exception("Dissolve error")

        # Should raise the exception
        with pytest.raises(Exception) as excinfo:
            silver_source._create_dissolved_df(sample_geodataframe, "test_dataset")

        assert "Dissolve error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_run_success(silver_source: WaterProjectsSilver) -> None:
    """Test run with successful processing."""
    # Mock data for testing
    bronze_df = pd.DataFrame(
        {
            "layer": ["test_layer"],
            "payload": [
                "<wfs:FeatureCollection xmlns:wfs='http://www.opengis.net/wfs/2.0'></wfs:FeatureCollection>"
            ],
        }
    )
    processed_gdf = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])], crs="EPSG:25832"
    )
    dissolved_gdf = gpd.GeoDataFrame(
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])], crs="EPSG:4326"
    )

    # Setup mocks
    with (
        patch.object(silver_source, "_read_bronze_data", return_value=bronze_df),
        patch.object(silver_source, "_process_data", return_value=processed_gdf),
        patch.object(silver_source, "_create_dissolved_df", return_value=dissolved_gdf),
        patch.object(silver_source, "_save_data") as mock_save_data,
    ):
        await silver_source.run()

        # Verify method calls
        silver_source._read_bronze_data.assert_called_once_with(
            silver_source.config.dataset, silver_source.config.bucket
        )
        silver_source._process_data.assert_called_once_with(bronze_df)
        silver_source._create_dissolved_df.assert_called_once_with(
            processed_gdf, silver_source.config.dataset
        )

        # Verify save calls
        assert mock_save_data.call_count == 2
        mock_save_data.assert_has_calls(
            [
                call(processed_gdf, silver_source.config.dataset, silver_source.config.bucket),
                call(
                    dissolved_gdf,
                    f"{silver_source.config.dataset}_dissolved",
                    silver_source.config.bucket,
                ),
            ]
        )


@pytest.mark.asyncio
async def test_run_no_bronze_data(silver_source: WaterProjectsSilver) -> None:
    """Test run when no bronze data is available."""
    # Setup mocks
    with patch.object(silver_source, "_read_bronze_data", return_value=None):
        await silver_source.run()

        # Verify method calls
        silver_source._read_bronze_data.assert_called_once()


@pytest.mark.asyncio
async def test_run_processing_failure(silver_source: WaterProjectsSilver) -> None:
    """Test run when processing fails."""
    # Mock data for testing
    bronze_df = pd.DataFrame(
        {
            "layer": ["test_layer"],
            "payload": [
                "<wfs:FeatureCollection xmlns:wfs='http://www.opengis.net/wfs/2.0'></wfs:FeatureCollection>"
            ],
        }
    )

    # Setup mocks
    with (
        patch.object(silver_source, "_read_bronze_data", return_value=bronze_df),
        patch.object(silver_source, "_process_data", return_value=None),
    ):
        await silver_source.run()

        # Verify method calls
        silver_source._read_bronze_data.assert_called_once()
        silver_source._process_data.assert_called_once()


def test_parse_geometry_missing_poslist(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with missing posList element."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <!-- Missing posList element -->
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(xml_string)
    result = silver_source._parse_geometry(geom_elem)
    assert result is None


def test_parse_geometry_empty_poslist(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with empty posList text."""
    xml_string = """
    <test:the_geom xmlns:test="http://test.namespace" 
                 xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList></gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(xml_string)
    result = silver_source._parse_geometry(geom_elem)
    assert result is None


def test_parse_geometry_general_exception(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_geometry with a general exception."""
    with patch.object(silver_source, "config") as mock_config:
        # Make config.gml_ns raise an exception when accessed
        type(mock_config).gml_ns = PropertyMock(side_effect=Exception("Test exception"))

        geom_elem = ET.fromstring(
            "<test:the_geom xmlns:test='http://test.namespace'></test:the_geom>"
        )
        result = silver_source._parse_geometry(geom_elem)

        assert result is None


def test_parse_feature_general_exception(silver_source: WaterProjectsSilver) -> None:
    """Test _parse_feature with a general exception."""
    # Create a mock feature that raises an exception when accessed
    mock_feature = MagicMock()
    mock_feature.tag = "test:Feature"
    # Make find() raise an exception
    mock_feature.find.side_effect = Exception("Test feature exception")

    result = silver_source._parse_feature(mock_feature)

    # Verify the exception was handled correctly
    assert result is None


def test_create_dissolved_df_invalid_single_polygon(silver_source: WaterProjectsSilver) -> None:
    """Test _create_dissolved_df with an invalid single polygon."""
    # Create a GeoDataFrame with a polygon
    polygon = Polygon([(10, 10), (20, 10), (20, 20), (10, 20)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[polygon], crs="EPSG:25832")

    # Create a mock polygon for better control
    mock_polygon = MagicMock()
    # Set geom_type property
    type(mock_polygon).geom_type = PropertyMock(return_value="Polygon")
    # Set is_valid and is_simple properties
    type(mock_polygon).is_valid = PropertyMock(return_value=False)
    type(mock_polygon).is_simple = PropertyMock(return_value=False)

    # Create mock exterior with coords
    mock_exterior = MagicMock()
    coords = [(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)]
    type(mock_exterior).coords = PropertyMock(return_value=coords)
    type(mock_polygon).exterior = PropertyMock(return_value=mock_exterior)

    # Create mock interior with coords
    mock_interior = MagicMock()
    int_coords = [(0.2, 0.2), (0.3, 0.2), (0.3, 0.3), (0.2, 0.3)]
    type(mock_interior).coords = PropertyMock(return_value=int_coords)
    type(mock_polygon).interiors = PropertyMock(return_value=[mock_interior])

    # Set up buffer method to return a valid polygon
    mock_polygon.buffer.return_value = polygon

    with (
        patch("unified_pipeline.silver.water_projects.unary_union") as mock_unary_union,
        patch("unified_pipeline.silver.water_projects.explain_validity") as mock_explain,
        patch(
            "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
        ) as mock_validate,
    ):
        # Setup mocks
        mock_unary_union.return_value = mock_polygon
        mock_explain.return_value = "Self-intersection at or near point 0.5 0.5"

        # Setup validate_and_transform_geometries mock
        mock_validate.return_value = gpd.GeoDataFrame(
            geometry=[polygon],  # Use a valid polygon for the result
            crs="EPSG:4326",
        )

        result = silver_source._create_dissolved_df(gdf, "test_dataset")

        # Verify result
        assert result is not None


def test_create_dissolved_df_invalid_polygon_case(silver_source: WaterProjectsSilver) -> None:
    """Test _create_dissolved_df with an invalid single polygon."""
    # Create a GeoDataFrame with a polygon
    polygon = Polygon([(10, 10), (20, 10), (20, 20), (10, 20)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[polygon], crs="EPSG:25832")

    # Create a mock polygon for better control
    mock_polygon = MagicMock()
    # Set geom_type property
    type(mock_polygon).geom_type = PropertyMock(return_value="Polygon")
    # Set is_valid and is_simple properties
    type(mock_polygon).is_valid = PropertyMock(return_value=False)
    type(mock_polygon).is_simple = PropertyMock(return_value=False)

    # Create mock exterior with coords
    mock_exterior = MagicMock()
    coords = [(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)]
    type(mock_exterior).coords = PropertyMock(return_value=coords)
    type(mock_polygon).exterior = PropertyMock(return_value=mock_exterior)

    # Create mock interior with coords
    mock_interior = MagicMock()
    int_coords = [(0.2, 0.2), (0.3, 0.2), (0.3, 0.3), (0.2, 0.3)]
    type(mock_interior).coords = PropertyMock(return_value=int_coords)
    type(mock_polygon).interiors = PropertyMock(return_value=[mock_interior])

    # Set up buffer method to return a valid polygon
    mock_polygon.buffer.return_value = polygon

    with (
        patch("unified_pipeline.silver.water_projects.unary_union") as mock_unary_union,
        patch("unified_pipeline.silver.water_projects.explain_validity") as mock_explain,
        patch(
            "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
        ) as mock_validate,
    ):
        # Setup mocks
        mock_unary_union.return_value = mock_polygon
        mock_explain.return_value = "Self-intersection at or near point 0.5 0.5"

        # Setup validate_and_transform_geometries mock
        mock_validate.return_value = gpd.GeoDataFrame(
            geometry=[polygon],  # Use a valid polygon for the result
            crs="EPSG:4326",
        )

        result = silver_source._create_dissolved_df(gdf, "test_dataset")

        # Verify result
        assert result is not None


def test_create_dissolved_df_invalid_single_polygon_with_interiors(
    silver_source: WaterProjectsSilver,
) -> None:
    """Test creating a dissolved DataFrame with a single invalid polygon with interiors for detailed logging."""  # noqa: E501
    # Create a mock polygon that fails validation
    invalid_polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    # Create a GeoDataFrame with the invalid polygon
    gdf = gpd.GeoDataFrame(geometry=[invalid_polygon], crs="EPSG:4326")

    # Mock the unary_union method to return an invalid polygon
    with (
        patch("shapely.ops.unary_union") as mock_unary_union,
        patch("shapely.validation.explain_validity") as mock_explain,
        patch(
            "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
        ) as mock_validate,
    ):
        # Mock an invalid polygon with interiors to trigger the detailed logging
        mock_polygon = MagicMock(spec=Polygon)

        # Make is_valid and is_simple properties return False
        type(mock_polygon).is_valid = PropertyMock(return_value=False)
        type(mock_polygon).is_simple = PropertyMock(return_value=False)

        # Set geom_type property
        type(mock_polygon).geom_type = PropertyMock(return_value="Polygon")

        # Create mock exterior with coords
        mock_exterior = MagicMock()
        coords = [(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)]
        type(mock_exterior).coords = PropertyMock(return_value=coords)
        type(mock_polygon).exterior = PropertyMock(return_value=mock_exterior)

        # Create mock interior with coords
        mock_interior = MagicMock()
        int_coords = [(0.2, 0.2), (0.3, 0.2), (0.3, 0.3), (0.2, 0.3)]
        type(mock_interior).coords = PropertyMock(return_value=int_coords)
        type(mock_polygon).interiors = PropertyMock(return_value=[mock_interior])

        # Setup unary_union mock to return our invalid polygon
        mock_unary_union.return_value = mock_polygon
        mock_explain.return_value = "Self-intersection at or near point 0.5 0.5"

        # Set up the final result after validation/transformation
        mock_validate.return_value = gpd.GeoDataFrame(geometry=[invalid_polygon], crs="EPSG:4326")

        # Call the method
        result = silver_source._create_dissolved_df(gdf, "test_dataset")

        # Verify the result
        assert result is not None


def test_parse_geometry_no_polygon_correct(silver_source: WaterProjectsSilver) -> None:
    """Test parsing geometry with a surface_member that doesn't have a Polygon element."""
    # Create a MultiSurface element with a surfaceMember that doesn't have a Polygon
    # and another surfaceMember with a valid Polygon
    geom_xml = """
    <test:the_geom xmlns:test="http://test.namespace" 
                xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <!-- No Polygon element here -->
            </gml:surfaceMember>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                10.0 10.0 20.0 10.0 20.0 20.0 10.0 20.0 10.0 10.0
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </test:the_geom>
    """
    geom_elem = ET.fromstring(geom_xml)

    # Parse the geometry
    result = silver_source._parse_geometry(geom_elem)

    # Verify the result (the second polygon should be processed correctly)
    assert result is not None
    assert "wkt" in result
    assert "POLYGON" in result["wkt"]
    assert result["area_ha"] > 0


def test_create_dissolved_df_invalid_polygon_with_interiors(
    silver_source: WaterProjectsSilver,
) -> None:
    """Test creating a dissolved DataFrame with a single invalid polygon with interior rings."""
    # Create a GeoDataFrame with a polygon
    polygon = Polygon([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[polygon], crs="EPSG:25832")

    # Mock the unary_union method to return an invalid polygon
    with (
        patch("unified_pipeline.silver.water_projects.unary_union") as mock_unary_union,
        patch("unified_pipeline.silver.water_projects.explain_validity") as mock_explain,
        patch(
            "unified_pipeline.util.geometry_validator.validate_and_transform_geometries"
        ) as mock_validate,
    ):
        # Create a mock polygon with interiors to trigger the detailed logging
        mock_polygon = MagicMock(spec=Polygon)

        # Make is_valid and is_simple properties return False
        type(mock_polygon).is_valid = PropertyMock(return_value=False)
        type(mock_polygon).is_simple = PropertyMock(return_value=False)
        type(mock_polygon).geom_type = PropertyMock(return_value="Polygon")

        # Create mock exterior with coords
        mock_exterior = MagicMock()
        coords = [(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)]
        type(mock_exterior).coords = PropertyMock(return_value=coords)
        type(mock_polygon).exterior = PropertyMock(return_value=mock_exterior)

        # Create mock interior with coords
        mock_interior = MagicMock()
        int_coords = [(0.2, 0.2), (0.3, 0.2), (0.3, 0.3), (0.2, 0.3), (0.2, 0.2)]
        type(mock_interior).coords = PropertyMock(return_value=int_coords)
        type(mock_polygon).interiors = PropertyMock(return_value=[mock_interior])

        # Make buffer() return a valid polygon
        mock_polygon.buffer.return_value = polygon

        # Setup mocks
        mock_unary_union.return_value = mock_polygon
        mock_explain.return_value = "Self-intersection at or near point 0.5 0.5"

        # Setup validate_and_transform_geometries mock
        mock_validate.return_value = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

        # Call the method
        result = silver_source._create_dissolved_df(gdf, "test_dataset")

        # Verify the result
        assert result is not None
