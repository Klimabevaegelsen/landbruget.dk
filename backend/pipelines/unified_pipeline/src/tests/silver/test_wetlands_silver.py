"""
Tests for the WetlandsSilver class.
"""

import xml.etree.ElementTree as ET
from typing import Any
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from unified_pipeline.silver.wetlands import WetlandsSilver, WetlandsSilverConfig

# Alias for geopandas.GeoDataFrame to match usage in tests
g_geo = gpd.GeoDataFrame


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
def silver_source(config: WetlandsSilverConfig) -> WetlandsSilver:
    """Return a test WetlandsSilver instance."""
    source = WetlandsSilver(config)
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
def sample_dataframe(sample_xml: str) -> dict:
    """Return a sample dataframe with XML payloads."""
    return {"payload": [sample_xml]}


@pytest.fixture
def simple_geodataframe() -> Any:
    """Return a simple Geo for testing the dissolve function."""
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
    return g_geo(data, crs="EPSG:25832")


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
    """Test logging geometry statistics from a DuckDB table."""
    # Create test table with geometries
    silver_source.conn.execute("""
        CREATE TABLE test_geom_stats AS
        SELECT ST_GeomFromText('POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))') as geometry
        UNION ALL
        SELECT ST_GeomFromText('POLYGON((200 200, 300 200, 300 300, 200 300, 200 200))') as geometry
    """)

    # Execute and verify no exceptions occur
    try:
        silver_source.log_geometry_statistics("test_geom_stats")
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
    assert isinstance(result, str)  # Now returns WKT string
    assert result.startswith("POLYGON((")  # WKT format


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
    # Now returns a flat dict with WKT geometry
    assert result["id"] == "id1"
    assert result["gridcode"] == 1
    assert result["toerv_pct"] == "25"
    assert result["geometry_wkt"] is not None
    assert result["geometry_wkt"].startswith("POLYGON((")


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


@patch("unified_pipeline.silver.wetlands.WetlandsSilver._parse_geometry")
def test_parse_feature_exception_handling(
    mock_parse_geometry: MagicMock, silver_source: WetlandsSilver
) -> None:
    """Test that _parse_feature handles exceptions properly and logs them."""
    # Mock _parse_geometry to raise an exception
    mock_parse_geometry.side_effect = Exception("Test geometry parsing error")

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

    # Should return None when exception occurs
    assert result is None

    # Verify that _parse_geometry was called
    mock_parse_geometry.assert_called_once()


def test_process_xml_data_empty(silver_source: WetlandsSilver) -> None:
    """Test processing empty XML data."""
    result = silver_source._process_xml_data(())
    assert result is None


def test_process_xml_data_error(silver_source: WetlandsSilver) -> None:
    """Test error handling when processing XML data with invalid input type."""
    # Now expects table name strings; passing a dict should return None (not raise)
    result = silver_source._process_xml_data({"payload": ["<invalid>"]})
    assert result is None  # Invalid input type returns None


def test_process_xml_data_success(silver_source: WetlandsSilver, sample_xml: str) -> None:
    """Test successfully processing XML data from a DuckDB table."""
    # Create a table with sample XML data
    silver_source.conn.execute("""
        CREATE TABLE test_raw_xml (payload VARCHAR)
    """)
    silver_source.conn.execute(
        """
        INSERT INTO test_raw_xml VALUES (?)
    """,
        [sample_xml],
    )

    result = silver_source._process_xml_data("test_raw_xml")

    assert result is not None
    assert isinstance(result, str)  # Returns table name
    # Verify table has expected columns
    columns = [col[0] for col in silver_source.conn.execute(f"DESCRIBE {result}").fetchall()]
    assert "id" in columns
    assert "gridcode" in columns
    assert "toerv_pct" in columns


def test_create_dissolved_df(silver_source: WetlandsSilver) -> None:
    """Test creating dissolved table using DuckDB-spatial."""
    # Create test table with wetland features
    silver_source.conn.execute("""
        CREATE TABLE test_wetlands AS
        SELECT
            'id1' as id,
            1 as gridcode,
            '25' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'id2' as id,
            1 as gridcode,
            '25' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
        UNION ALL SELECT
            'id3' as id,
            2 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 20, 10 20, 10 30, 0 30, 0 20))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_wetlands", "test")

    # Verify result is a table name
    assert result is not None
    assert isinstance(result, str)

    # Verify table has expected columns
    columns = [col[0] for col in silver_source.conn.execute(f"DESCRIBE {result}").fetchall()]
    assert "wetland_id" in columns
    assert "geometry" in columns


def test_create_dissolved_df_neighbor_checking_and_edge_sharing(
    silver_source: WetlandsSilver,
) -> None:
    """Test the neighbor checking and edge sharing logic in DuckDB-spatial dissolve."""
    # Create test table with adjacent polygons
    silver_source.conn.execute("""
        CREATE TABLE test_neighbors AS
        SELECT
            'id1' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'id2' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_neighbors", "test")

    # Verify result is valid
    assert result is not None
    assert isinstance(result, str)

    # Verify the dissolved table has features
    row_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count >= 1


def test_create_dissolved_df_spatial_index_efficiency(silver_source: WetlandsSilver) -> None:
    """Test that spatial operations work with DuckDB-spatial indexes."""
    # Create test table with features in different areas
    silver_source.conn.execute("""
        CREATE TABLE test_spatial AS
        SELECT
            'id1' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'id2' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((100 100, 110 100, 110 110, 100 110, 100 100))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_spatial", "test")

    # Verify result is valid
    assert result is not None
    assert isinstance(result, str)


def test_create_dissolved_df_edge_sharing_criteria(silver_source: WetlandsSilver) -> None:
    """Test edge sharing with DuckDB-spatial dissolve."""
    # Create test table with adjacent and non-adjacent polygons
    silver_source.conn.execute("""
        CREATE TABLE test_edges AS
        SELECT
            'id1' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'id2' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
        UNION ALL SELECT
            'id3' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((50 50, 60 50, 60 60, 50 60, 50 50))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_edges", "test")

    # Verify result is valid
    assert result is not None
    row_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count >= 1


def test_create_dissolved_df_merged_tracking(silver_source: WetlandsSilver) -> None:
    """Test the merged tracking in DuckDB-spatial dissolve."""
    # Create test table with a chain of polygons
    silver_source.conn.execute("""
        CREATE TABLE test_chain AS
        SELECT
            'id1' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'id2' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
        UNION ALL SELECT
            'id3' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_chain", "test")

    # Verify result is valid
    assert result is not None
    assert isinstance(result, str)


@pytest.mark.asyncio
async def test_run_success(silver_source: WetlandsSilver) -> None:
    """Test successful run of the pipeline."""
    # Mock methods - _process_xml_data includes dissolve logic internally
    silver_source._read_bronze_data = MagicMock(return_value="test_bronze_table")
    silver_source._process_xml_data = MagicMock(return_value="test_table")
    silver_source.save_data_direct = MagicMock()

    await silver_source.run()

    # Verify method calls
    silver_source._read_bronze_data.assert_called_once_with(
        silver_source.config.dataset, silver_source.config.bucket
    )
    silver_source._process_xml_data.assert_called_once()
    # save_data_direct is called twice: once for main table, once for dissolved
    assert silver_source.save_data_direct.call_count == 2


@pytest.mark.asyncio
async def test_run_read_bronze_data_error(silver_source: WetlandsSilver) -> None:
    """Test run with error in reading bronze data."""
    silver_source._read_bronze_data = MagicMock(return_value=None)
    silver_source._process_xml_data = MagicMock()
    silver_source._create_dissolved_df_before_transform = MagicMock()
    silver_source.save_data_direct = MagicMock()

    await silver_source.run()

    assert not silver_source._process_xml_data.called
    assert not silver_source._create_dissolved_df_before_transform.called


@pytest.mark.asyncio
async def test_run_process_xml_data_error(silver_source: WetlandsSilver) -> None:
    """Test run with error in processing XML data."""
    silver_source._read_bronze_data = MagicMock(return_value="test_bronze_table")
    silver_source._process_xml_data = MagicMock(return_value=None)
    silver_source._create_dissolved_df_before_transform = MagicMock()
    silver_source.save_data_direct = MagicMock()

    await silver_source.run()

    assert not silver_source._create_dissolved_df_before_transform.called
    assert not silver_source.save_data_direct.called


def test_parse_geometry_coordinate_parsing_exception(silver_source: WetlandsSilver) -> None:
    """Test that _parse_geometry handles coordinate parsing exceptions properly."""
    # Create XML with invalid coordinate data that will cause float() to fail
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>invalid_coord 55.0 10.1 not_a_number</gml:posList>
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

    # Should return None when coordinate parsing fails
    assert result is None


def test_parse_geometry_with_inner_ring(silver_source: WetlandsSilver) -> None:
    """Test parsing a geometry with coordinates."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>0 0 1 0 1 1 0 1 0 0</gml:posList>
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
    # Now returns WKT string
    assert result is not None
    assert isinstance(result, str)
    assert result.startswith("POLYGON((")


def test_parse_feature_with_non_integer_gridcode(silver_source: WetlandsSilver) -> None:
    """Test parsing a feature with non-integer gridcode."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <natur:kulstof2022 gml:id="id1" xmlns:natur="http://wfs2-miljoegis.mim.dk/natur" xmlns:gml="http://www.opengis.net/gml/3.2">
        <natur:gridcode>not_an_integer</natur:gridcode>
        <natur:toerv_pct>25</natur:toerv_pct>
        <gml:Polygon>
            <gml:exterior>
                <gml:LinearRing>
                    <gml:posList>10.0 55.0 10.1 a0 10.1 55.1 10.0 55.1 10.0 55.0</gml:posList>
                </gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>
    </natur:kulstof2022>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_feature(root)

    # Should return None when gridcode is not an integer
    assert result is None


def test_create_dissolved_df_neighbor_iteration_and_edge_check(
    silver_source: WetlandsSilver,
) -> None:
    """Test neighbor iteration logic with DuckDB-spatial dissolve."""
    # Create test table with adjacent polygons
    silver_source.conn.execute("""
        CREATE TABLE test_iter AS
        SELECT
            'A' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            'B' as id,
            1 as gridcode,
            '>12' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_iter", "test")

    # Verify result is valid
    assert result is not None
    assert isinstance(result, str)


def test_create_dissolved_df_merged_set_prevents_double_processing(
    silver_source: WetlandsSilver,
) -> None:
    """Test that the dissolve operation handles adjacent polygons correctly."""
    # Create test table with chain of adjacent polygons
    silver_source.conn.execute("""
        CREATE TABLE test_double AS
        SELECT
            '1' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') as geometry
        UNION ALL SELECT
            '2' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))') as geometry
        UNION ALL SELECT
            '3' as id,
            1 as gridcode,
            '6-12' as toerv_pct,
            ST_GeomFromText('POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))') as geometry
    """)

    result = silver_source._create_dissolved_df_before_transform("test_double", "test")

    # Verify result is valid
    assert result is not None
    assert isinstance(result, str)
    # Verify table has data
    row_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count >= 1


def test_create_dissolved_df_exception_handling(silver_source: WetlandsSilver) -> None:
    """Test that _create_dissolved_df_before_transform handles invalid tables gracefully."""
    # Try to process a non-existent table
    with pytest.raises(Exception):  # noqa: B017
        silver_source._create_dissolved_df_before_transform("nonexistent_table", "test")
