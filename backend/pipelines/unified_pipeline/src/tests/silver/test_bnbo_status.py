import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest

from unified_pipeline.silver.bnbo_status import BNBOStatusSilver, BNBOStatusSilverConfig

# Create alias for test compatibility
g_geo = gpd.GeoDataFrame


def get_current_working_directory() -> str:
    import os

    return os.getcwd()


@pytest.fixture
def silver_config() -> BNBOStatusSilverConfig:
    return BNBOStatusSilverConfig()


@pytest.fixture
def bnbo_status_silver(silver_config: BNBOStatusSilverConfig) -> BNBOStatusSilver:
    return BNBOStatusSilver(silver_config)


def test_bnbo_status_silver_config(silver_config: BNBOStatusSilverConfig) -> None:
    assert silver_config.dataset == "bnbo_status"
    assert silver_config.bucket == "landbrugsdata-raw-data"
    assert silver_config.status_mapping["Indsats gennemført"] == "Completed"
    assert silver_config.gml_ns == "{http://www.opengis.net/gml/3.2}"


def test_read_bronze_data_with_in_memory_data(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test _read_bronze_data with in-memory data (the preferred path)."""
    # Provide in-memory bronze data (this is the modern approach)
    bronze_data = ["<xml>test data</xml>"]

    result = bnbo_status_silver._read_bronze_data(
        silver_config.dataset, silver_config.bucket, bronze_data=bronze_data
    )

    # Should return a table name
    assert result is not None
    assert isinstance(result, str)

    # Verify the table was created and has the expected data
    row_count = bnbo_status_silver.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count == 1

    # Check the payload was stored correctly
    payload = bnbo_status_silver.conn.execute(f"SELECT payload FROM {result}").fetchone()[0]
    assert payload == "<xml>test data</xml>"


def test_read_bronze_data_no_data(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test _read_bronze_data with no bronze data provided."""
    # Don't provide bronze_data, and mock the storage fallback to return None
    with patch.object(bnbo_status_silver, "_read_bronze_data_from_storage", return_value=None):
        result = bnbo_status_silver._read_bronze_data(silver_config.dataset, silver_config.bucket)
        assert result is None


def test_get_first_namespace(bnbo_status_silver: BNBOStatusSilver) -> None:
    xml_string = '<ns1:root xmlns:ns1="http://example.com/ns1"><ns1:child/></ns1:root>'
    root = ET.fromstring(xml_string)
    assert bnbo_status_silver.get_first_namespace(root) == "http://example.com/ns1"

    xml_string_no_ns = "<root><child/></root>"
    root_no_ns = ET.fromstring(xml_string_no_ns)
    assert bnbo_status_silver.get_first_namespace(root_no_ns) is None


def test_clean_value(bnbo_status_silver: BNBOStatusSilver) -> None:
    assert bnbo_status_silver.clean_value("  test  ") == "test"
    assert bnbo_status_silver.clean_value("  ") is None
    assert bnbo_status_silver.clean_value(123) == "123"
    assert bnbo_status_silver.clean_value(None) == "None"


def test_parse_geometry_valid(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a valid geometry from XML"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <Shape xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>0 0 1 1 1 0 0 0</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </Shape>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is not None
    assert "wkt" in result
    assert "area_ha" in result
    assert result["area_ha"] > 0  # Area should be positive


def test_parse_geometry_multiple_polygons(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a geometry with multiple polygons"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <Shape xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>0 0 1 1 1 0 0 0</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>2 2 3 3 3 2 2 2</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </Shape>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is not None
    assert "wkt" in result
    assert "POLYGON" in result["wkt"]


def test_parse_geometry_no_multi_surface(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a geometry without MultiSurface element"""
    xml_string = "<Shape><InvalidElement>test</InvalidElement></Shape>"
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is None


def test_parse_geometry_invalid_coordinates(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a geometry with invalid coordinates"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <Shape xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>invalid coordinates</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </Shape>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is None


def test_parse_geometry_without_polygon(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a geometry without Polygon element"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <Shape xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:MultiSurface>
            <gml:surfaceMember>
                <gml:InvalidElement>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>0 0 1 1 1 0 0 0</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </gml:InvalidElement>
            </gml:surfaceMember>
        </gml:MultiSurface>
    </Shape>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is None


def test_parse_geometry_no_coordinates(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a geometry with no coordinates"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <Shape xmlns:gml="http://www.opengis.net/gml/3.2">
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
    </Shape>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_geometry(root)
    assert result is None


def test_parse_geometry_exception_handling(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test error handling in _parse_geometry method."""
    mock_element = MagicMock()
    mock_element.find.side_effect = Exception("Test exception")

    result = bnbo_status_silver._parse_geometry(mock_element)
    assert result is None


def test_parse_feature(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a feature from XML"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <gml:Feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Shape>
            <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>0 0 1 1 1 0 0 0</gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
            </gml:MultiSurface>
        </gml:Shape>
        <gml:status_bnbo>unknown</gml:status_bnbo>
    </gml:Feature>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_feature(root)

    assert result is not None
    assert "geometry" in result
    assert "area_ha" in result
    assert "status_bnbo" in result
    assert "status_category" in result
    assert result["status_bnbo"] == "unknown"
    assert result["status_category"] == "Unknown"
    assert result["area_ha"] > 0  # Area should be positive


def test_parse_feature_with_no_shape(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a feature without Shape element"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <gml:Feature xmlns:gml="http://www.opengis.net/gml/3.2">
    </gml:Feature>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_feature(root)
    assert result is None


def test_parse_feature_with_invalid_shape(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test parsing a feature with invalid Shape element"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <gml:Feature xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:Shape>
            <InvalidElement>test</InvalidElement>
        </gml:Shape>
    </gml:Feature>
    """
    root = ET.fromstring(xml_string)
    result = bnbo_status_silver._parse_feature(root)
    assert result is None


def test_parse_feature_with_exception_handling(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test error handling in _parse_feature method."""
    mock_element = MagicMock()
    mock_element.find.side_effect = Exception("Test exception")

    result = bnbo_status_silver._parse_feature(mock_element)
    assert result is None


def test_process_xml_data(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test processing XML data"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <gml:FeatureCollection xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:member>
            <gml:Feature>
                <gml:Shape>
                    <gml:MultiSurface>
                        <gml:surfaceMember>
                            <gml:Polygon>
                                <gml:exterior>
                                    <gml:LinearRing>
                                        <gml:posList>0 0 1 1 1 0 0 0</gml:posList>
                                    </gml:LinearRing>
                                </gml:exterior>
                            </gml:Polygon>
                        </gml:surfaceMember>
                    </gml:MultiSurface>
                </gml:Shape>
                <status_bnbo>Indsats gennemført</status_bnbo>
            </gml:Feature>
        </gml:member>
    </gml:FeatureCollection>
    """
    df = {"payload": [xml_string]}

    result = bnbo_status_silver._process_xml_data(df)

    assert result is not None
    assert isinstance(result, str)  # Returns table name

    # Verify the table was created and has expected data
    row_count = bnbo_status_silver.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count == 1

    # Check columns exist
    columns = [desc[0] for desc in bnbo_status_silver.conn.execute(f"DESCRIBE {result}").fetchall()]
    assert "geometry" in columns
    assert "area_ha" in columns
    assert "status_bnbo" in columns
    assert "status_category" in columns

    # Check data values
    data = bnbo_status_silver.conn.execute(
        f"SELECT status_bnbo, status_category FROM {result}"
    ).fetchone()
    assert data[0] == "Indsats gennemført"
    assert data[1] == "Completed"


def test_process_xml_data_with_empty_dataframe(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test processing an empty dict"""
    df = {"payload": []}
    result = bnbo_status_silver._process_xml_data(df)
    assert result is None


def test_process_xml_data_with_no_namespace(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test processing XML data without namespace"""
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
    <FeatureCollection>
        <member>
        </member>
    </FeatureCollection>
    """
    df = {"payload": [xml_string]}
    with pytest.raises(Exception) as excinfo:
        bnbo_status_silver._process_xml_data(df)
        assert "No namespace found in XML" in str(excinfo.value)


def test_create_dissolved_df_with_test_data(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test _create_dissolved_df with DuckDB table input."""
    # Create test table with sample data
    input_table = "test_input_table"
    bnbo_status_silver.conn.execute(f"""
        CREATE TABLE {input_table} AS
        SELECT
            'Action Required' as status_category,
            ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))') as geometry_spatial
        UNION ALL
        SELECT
            'Completed' as status_category,
            ST_GeomFromText('POLYGON((3 3, 3 5, 5 5, 5 3, 3 3))') as geometry_spatial
    """)

    # Call the method
    result_table = bnbo_status_silver._create_dissolved_df(input_table, "test_dataset")

    # Verify result table exists and has expected structure
    assert result_table is not None
    assert isinstance(result_table, str)

    # Check the table has data
    row_count = bnbo_status_silver.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[
        0
    ]
    assert row_count > 0

    # Check expected columns exist
    columns = [
        desc[0] for desc in bnbo_status_silver.conn.execute(f"DESCRIBE {result_table}").fetchall()
    ]
    assert "status_category" in columns
    assert "dissolved_geometry" in columns


def test_create_dissolved_df_empty_input(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test _create_dissolved_df with empty table."""
    # Create empty table
    input_table = "test_empty_table"
    bnbo_status_silver.conn.execute(f"""
        CREATE TABLE {input_table} AS
        SELECT
            CAST(NULL AS VARCHAR) as status_category,
            CAST(NULL AS GEOMETRY) as geometry_spatial
        WHERE FALSE
    """)

    # Call the method
    result_table = bnbo_status_silver._create_dissolved_df(input_table, "test_dataset_empty")

    # Should return a table name even for empty input
    assert result_table is not None
    assert isinstance(result_table, str)


def test_save_data(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test saving data to GCS."""
    # Create a sample table in DuckDB
    table_name = "test_bnbo_table"
    bnbo_status_silver.conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT 'Action Required' as status_category,
               ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') as geometry
    """)

    # Mock the save_data_direct method
    with patch.object(bnbo_status_silver, "save_data_direct") as mock_save:
        bnbo_status_silver.save_data_direct(
            table_name, silver_config.dataset, silver_config.bucket, "silver"
        )
        mock_save.assert_called_once_with(
            table_name, silver_config.dataset, silver_config.bucket, "silver"
        )


def test_save_data_with_empty_dataframe(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test saving an empty table to GCS."""
    # Create an empty table in DuckDB
    table_name = "test_empty_table"
    bnbo_status_silver.conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT CAST(NULL AS VARCHAR) as status_category,
               CAST(NULL AS GEOMETRY) as geometry
        WHERE FALSE
    """)

    # Mock the save_data_direct method
    with patch.object(bnbo_status_silver, "save_data_direct") as mock_save:
        bnbo_status_silver.save_data_direct(
            table_name, silver_config.dataset, silver_config.bucket, "silver"
        )
        mock_save.assert_called_once_with(
            table_name, silver_config.dataset, silver_config.bucket, "silver"
        )


@pytest.mark.asyncio
async def test_run(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test the run method."""
    # Mock the read_data and save_data methods
    with (
        patch.object(
            bnbo_status_silver, "_read_bronze_data", return_value="bronze_table"
        ) as mock_read_data,
        patch.object(
            bnbo_status_silver, "_process_xml_data", return_value="processed_table"
        ) as mock_process_xml_data,
        patch.object(
            bnbo_status_silver, "_create_dissolved_df", return_value="dissolved_table"
        ) as mock_create_dissolved_df,
        patch.object(bnbo_status_silver, "save_data_direct") as mock_save_data,
    ):
        await bnbo_status_silver.run()

        # Check that the methods were called
        mock_read_data.assert_called_once_with(silver_config.dataset, silver_config.bucket)
        mock_process_xml_data.assert_called_once()
        mock_create_dissolved_df.assert_called_once()
        mock_save_data.assert_called()
        assert mock_save_data.call_count == 2


@pytest.mark.asyncio
async def test_run_with_empty_dataframe(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test the run method with an empty ."""
    # Mock the read_data and save_data methods
    with (
        patch.object(bnbo_status_silver, "_read_bronze_data", return_value=None) as mock_read_data,
    ):
        await bnbo_status_silver.run()

        # Check that the methods were called
        mock_read_data.assert_called_once_with(silver_config.dataset, silver_config.bucket)


@pytest.mark.asyncio
async def test_run_with_empty_processed_data(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test the run method with empty processed data."""
    # Mock the read_data and save_data methods
    with (
        patch.object(
            bnbo_status_silver, "_read_bronze_data", return_value="bronze_table"
        ) as mock_read_data,
        patch.object(
            bnbo_status_silver, "_process_xml_data", return_value=None
        ) as mock_process_xml_data,
    ):
        await bnbo_status_silver.run()

        # Check that the methods were called
        mock_read_data.assert_called_once_with(silver_config.dataset, silver_config.bucket)
        mock_process_xml_data.assert_called_once()
