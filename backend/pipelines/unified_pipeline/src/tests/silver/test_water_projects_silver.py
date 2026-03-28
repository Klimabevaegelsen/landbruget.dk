"""
Tests for the WaterProjectsSilver class.

This module tests the silver layer processing for water projects data,
updated to work with the refactored DuckDB-based implementation.
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from unified_pipeline.silver.water_projects import WaterProjectsSilver, WaterProjectsSilverConfig


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
def silver_source(config: WaterProjectsSilverConfig) -> WaterProjectsSilver:
    """Return a test WaterProjectsSilver instance."""
    with patch("unified_pipeline.common.base.StorageAccess"):
        return WaterProjectsSilver(config)


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


def test_config_defaults() -> None:
    """Test that the config defaults are set correctly."""
    config = WaterProjectsSilverConfig()
    assert config.dataset == "water_projects"
    assert config.bucket == "landbruget-data"
    assert config.storage_batch_size == 8000
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
    # Area is in square meters / 10000 for hectares
    assert result["area_ha"] >= 0


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
    assert result["area_ha"] >= 0


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
    assert result[0]["area_ha"] >= 0
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


def test_process_data_success(silver_source: WaterProjectsSilver) -> None:
    """Test _process_data with valid bronze data table."""
    # Create a test bronze data table with the expected schema
    silver_source.conn.execute("""
        CREATE TABLE test_bronze_data (
            layer VARCHAR,
            payload TEXT
        )
    """)

    # Insert test XML data
    xml_payload = """
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
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    silver_source.conn.execute(
        "INSERT INTO test_bronze_data (layer, payload) VALUES (?, ?)",
        ["test_layer1", xml_payload],
    )

    result_table = silver_source._process_data("test_bronze_data")

    # Verify result is a table name
    assert result_table is not None
    assert isinstance(result_table, str)

    # Verify table has data
    count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
    assert count >= 1


def test_process_data_empty_table(silver_source: WaterProjectsSilver) -> None:
    """Test _process_data with empty table returns None."""
    # Create an empty table
    silver_source.conn.execute("""
        CREATE TABLE empty_bronze_data (
            layer VARCHAR,
            payload TEXT
        )
    """)

    result = silver_source._process_data("empty_bronze_data")
    assert result is None


def test_process_data_no_features_extracted(silver_source: WaterProjectsSilver) -> None:
    """Test _process_data when no features are extracted."""
    # Create a table with invalid data that won't produce features
    silver_source.conn.execute("""
        CREATE TABLE invalid_bronze_data (
            layer VARCHAR,
            payload TEXT
        )
    """)

    # Insert data that won't produce valid features (XML with no geometry)
    xml_payload = """
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0">
        <wfs:member>
            <test:Feature xmlns:test="http://test.namespace">
                <test:id>123</test:id>
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    silver_source.conn.execute(
        "INSERT INTO invalid_bronze_data (layer, payload) VALUES (?, ?)",
        ["test_layer1", xml_payload],
    )

    result = silver_source._process_data("invalid_bronze_data")
    assert result is None


def test_create_dissolved_df_empty_table(silver_source: WaterProjectsSilver) -> None:
    """Test _create_dissolved_df with empty input table."""
    # Create an empty table with the expected structure
    silver_source.conn.execute("""
        CREATE TABLE empty_water_projects (
            geometry_spatial GEOMETRY,
            layer VARCHAR
        )
    """)

    result_table = silver_source._create_dissolved_df("empty_water_projects", "test_dataset")

    # Should return a table name even for empty input
    assert result_table is not None
    assert isinstance(result_table, str)


def test_create_dissolved_df_with_data(silver_source: WaterProjectsSilver) -> None:
    """Test _create_dissolved_df with valid geometry data."""
    # Create a table with valid geometry data
    silver_source.conn.execute("""
        CREATE TABLE test_water_projects (
            geometry_spatial GEOMETRY,
            layer VARCHAR
        )
    """)

    # Insert valid polygon geometries
    silver_source.conn.execute("""
        INSERT INTO test_water_projects (geometry_spatial, layer)
        VALUES
            (ST_GeomFromText('POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))'), 'layer1'),
            (ST_GeomFromText('POLYGON((15 15, 25 15, 25 25, 15 25, 15 15))'), 'layer2')
    """)

    with patch("unified_pipeline.silver.water_projects.validate_and_transform_geometries_duckdb"):
        result_table = silver_source._create_dissolved_df("test_water_projects", "test_dataset")

    # Should return a table name
    assert result_table is not None
    assert isinstance(result_table, str)
    assert "dissolved" in result_table


@pytest.mark.asyncio
async def test_run_success(silver_source: WaterProjectsSilver) -> None:
    """Test run with successful processing."""
    # Mock data for testing
    silver_source._read_bronze_data = MagicMock(return_value="mock_bronze_table")

    # Create mock bronze table
    silver_source.conn.execute("""
        CREATE TABLE mock_bronze_table (
            layer VARCHAR,
            payload TEXT
        )
    """)

    xml_payload = """
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
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    silver_source.conn.execute(
        "INSERT INTO mock_bronze_table (layer, payload) VALUES (?, ?)",
        ["test_layer1", xml_payload],
    )

    # Mock _save_data to avoid GCS operations
    silver_source._save_data = MagicMock()

    # Mock geometry validation
    with patch("unified_pipeline.silver.water_projects.validate_and_transform_geometries_duckdb"):
        result = await silver_source.run()

    # Verify _read_bronze_data was called
    silver_source._read_bronze_data.assert_called_once()

    # Verify _save_data was called (at least twice - for processed and dissolved)
    assert silver_source._save_data.call_count >= 2

    # Verify result contains expected data
    assert result is not None
    assert "processed_data" in result
    assert "dissolved_data" in result
    assert "dataset" in result


@pytest.mark.asyncio
async def test_run_no_bronze_data(silver_source: WaterProjectsSilver) -> None:
    """Test run when no bronze data is available."""
    # Mock _read_bronze_data to return None
    silver_source._read_bronze_data = MagicMock(return_value=None)

    result = await silver_source.run()

    # Verify _read_bronze_data was called
    silver_source._read_bronze_data.assert_called_once()

    # Result should be None when no bronze data
    assert result is None


@pytest.mark.asyncio
async def test_run_processing_failure(silver_source: WaterProjectsSilver) -> None:
    """Test run when processing fails."""
    # Create mock bronze table with invalid data
    silver_source._read_bronze_data = MagicMock(return_value="mock_empty_table")

    # Create an empty table (will result in processing failure)
    silver_source.conn.execute("""
        CREATE TABLE mock_empty_table (
            layer VARCHAR,
            payload TEXT
        )
    """)

    result = await silver_source.run()

    # Verify _read_bronze_data was called
    silver_source._read_bronze_data.assert_called_once()

    # Result should be None when processing fails
    assert result is None


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
    assert result["area_ha"] >= 0


@pytest.mark.asyncio
async def test_run_with_bronze_data_list(silver_source: WaterProjectsSilver) -> None:
    """Test run with bronze_data provided as list of tuples."""
    # Create bronze data as list of tuples (layer, data)
    xml_payload = """
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
            </test:Feature>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    bronze_data = [("test_layer1", xml_payload)]

    # Mock _save_data to avoid GCS operations
    silver_source._save_data = MagicMock()

    # Mock geometry validation
    with patch("unified_pipeline.silver.water_projects.validate_and_transform_geometries_duckdb"):
        result = await silver_source.run(bronze_data=bronze_data)

    # Verify result contains expected data
    assert result is not None
    assert "processed_data" in result
    assert "dissolved_data" in result
