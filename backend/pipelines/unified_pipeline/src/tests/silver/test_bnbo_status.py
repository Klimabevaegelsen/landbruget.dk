import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from unified_pipeline.silver.bnbo_status import BNBOStatusSilver, BNBOStatusSilverConfig
from unified_pipeline.util.gcs_util import GCSUtil


def get_current_working_directory() -> str:
    import os

    return os.getcwd()


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    return MagicMock(spec=GCSUtil)


@pytest.fixture
def silver_config() -> BNBOStatusSilverConfig:
    return BNBOStatusSilverConfig()


@pytest.fixture
def bnbo_status_silver(
    silver_config: BNBOStatusSilverConfig, mock_gcs_util: MagicMock
) -> BNBOStatusSilver:
    return BNBOStatusSilver(silver_config, mock_gcs_util)


def test_bnbo_status_silver_config(silver_config: BNBOStatusSilverConfig) -> None:
    assert silver_config.dataset == "bnbo_status"
    assert silver_config.bucket == "landbrugsdata-raw-data"
    assert silver_config.status_mapping["Indsats gennemført"] == "Completed"
    assert silver_config.gml_ns == "{http://www.opengis.net/gml/3.2}"


@patch("unified_pipeline.common.base.pd.Timestamp")
@patch("unified_pipeline.common.base.os.makedirs")
def test_read_data_success(
    mock_makedirs: MagicMock,
    mock_timestamp: MagicMock,
    bnbo_status_silver: BNBOStatusSilver,
    mock_gcs_util: MagicMock,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    mock_now = pd.Timestamp("2025-05-08")
    mock_timestamp.now.return_value = mock_now
    expected_date_str = mock_now.strftime("%Y-%m-%d")

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_util.get_gcs_client.return_value.bucket.return_value = mock_bucket

    dummy_df = pd.DataFrame({"payload": ["<xml></xml>"]})

    with patch(
        "unified_pipeline.common.base.pd.read_parquet", return_value=dummy_df
    ) as mock_read_parquet:
        result_df = bnbo_status_silver._read_bronze_data(
            silver_config.dataset, silver_config.bucket
        )

    mock_gcs_util.get_gcs_client.return_value.bucket.assert_called_once_with(silver_config.bucket)
    mock_bucket.blob.assert_called_once_with(
        f"bronze/{silver_config.dataset}/{expected_date_str}.parquet"
    )
    mock_blob.exists.assert_called_once()

    temp_dir = f"/tmp/bronze/{silver_config.dataset}"
    mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)
    temp_file = f"{temp_dir}/{expected_date_str}.parquet"
    mock_blob.download_to_filename.assert_called_once_with(temp_file)
    mock_read_parquet.assert_called_once_with(temp_file)

    assert result_df is not None
    pd.testing.assert_frame_equal(result_df, dummy_df)


@patch("unified_pipeline.silver.bnbo_status.pd.Timestamp")
def test_read_data_blob_not_exists(
    mock_timestamp: MagicMock,
    bnbo_status_silver: BNBOStatusSilver,
    mock_gcs_util: MagicMock,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    mock_now = pd.Timestamp("2025-05-08")
    mock_timestamp.now.return_value = mock_now

    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_util.get_gcs_client.return_value.bucket.return_value = mock_bucket

    result_df = bnbo_status_silver._read_bronze_data(silver_config.dataset, silver_config.bucket)

    assert result_df is None


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
    df = pd.DataFrame({"payload": [xml_string]})

    result = bnbo_status_silver._process_xml_data(df)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert "geometry" in result.columns
    assert "area_ha" in result.columns
    assert "status_bnbo" in result.columns
    assert "status_category" in result.columns
    assert result.shape[0] == 1
    assert result.shape[1] == 4
    assert result["status_bnbo"].iloc[0] == "Indsats gennemført"
    assert result["status_category"].iloc[0] == "Completed"


def test_process_xml_data_with_empty_dataframe(bnbo_status_silver: BNBOStatusSilver) -> None:
    """Test processing an empty DataFrame"""
    df = pd.DataFrame()
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
    df = pd.DataFrame({"payload": [xml_string]})
    with pytest.raises(Exception) as excinfo:
        bnbo_status_silver._process_xml_data(df)
        assert "No namespace found in XML" in str(excinfo.value)


@patch("unified_pipeline.silver.bnbo_status.validate_and_transform_geometries")
def test_create_dissolved_df_mixed_statuses(
    mock_validate_transform: MagicMock, bnbo_status_silver: BNBOStatusSilver
) -> None:
    """Test _create_dissolved_df with mixed statuses and overlapping geometries."""
    # Create sample data
    data = {
        "status_category": [
            "Action Required",
            "Action Required",
            "Completed",
            "Completed",
        ],
        "geometry": [
            Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),  # Overlaps with a completed
            Polygon([(3, 3), (3, 5), (5, 5), (5, 3)]),  # No overlap
            Polygon([(1, 1), (1, 3), (3, 3), (3, 1)]),  # Overlaps with an action_required
            Polygon([(6, 6), (6, 8), (8, 8), (8, 6)]),  # No overlap
        ],
    }
    input_gdf = gpd.GeoDataFrame(
        data, crs="EPSG:4326"
    )  # Use EPSG:4326 directly to avoid transformation issues

    # Mock validate_and_transform_geometries
    mock_validate_transform.side_effect = lambda gdf, _: gdf

    result_gdf = bnbo_status_silver._create_dissolved_df(input_gdf.copy(), "test_dataset_mixed")

    # Basic assertions
    assert result_gdf is not None
    assert not result_gdf.empty
    assert result_gdf.crs.to_epsg() == 4326  # type: ignore

    # Get filtered dataframes for each category
    action_required_gdf = result_gdf[result_gdf["status_category"] == "Action Required"]
    completed_gdf = result_gdf[result_gdf["status_category"] == "Completed"]

    assert not action_required_gdf.empty
    assert not completed_gdf.empty

    # Calculate expected number of polygons instead of area after dissolution
    assert len(action_required_gdf) == 2  # Two separate action required areas
    assert len(completed_gdf) == 2  # Two separate completed areas


@patch("unified_pipeline.silver.bnbo_status.validate_and_transform_geometries")
def test_create_dissolved_df_action_required_only(
    mock_validate_transform: MagicMock, bnbo_status_silver: BNBOStatusSilver
) -> None:
    """Test _create_dissolved_df with only Action Required statuses."""
    # Create sample data with only Action Required areas
    data = {
        "status_category": ["Action Required", "Action Required"],
        "geometry": [
            Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),  # Area 4.0
            Polygon([(1, 1), (1, 3), (3, 3), (3, 1)]),  # Area 4.0, overlaps with first
        ],
    }
    input_gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    mock_validate_transform.side_effect = lambda gdf, _: gdf

    result_gdf = bnbo_status_silver._create_dissolved_df(input_gdf.copy(), "test_dataset_ar_only")

    # Basic assertions
    assert result_gdf is not None
    assert not result_gdf.empty
    assert result_gdf.crs.to_epsg() == 4326  # type: ignore

    # Check that we get one dissolved polygon for Action Required and none for Completed
    assert len(result_gdf[result_gdf["status_category"] == "Completed"]) == 0
    assert (
        len(result_gdf[result_gdf["status_category"] == "Action Required"]) == 1
    )  # Dissolved to 1


@patch("unified_pipeline.silver.bnbo_status.validate_and_transform_geometries")
def test_create_dissolved_df_completed_only(
    mock_validate_transform: MagicMock, bnbo_status_silver: BNBOStatusSilver
) -> None:
    """Test _create_dissolved_df with only Completed statuses and different input CRS."""
    # Create sample data with only Completed areas
    data = {
        "status_category": ["Completed", "Completed"],
        "geometry": [
            Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),  # Area 4.0
            Polygon([(1, 1), (1, 3), (3, 3), (3, 1)]),  # Area 4.0, overlaps
        ],
    }
    input_gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")  # Use consistent CRS
    mock_validate_transform.side_effect = lambda gdf, _: gdf

    result_gdf = bnbo_status_silver._create_dissolved_df(input_gdf.copy(), "test_dataset_c_only")

    # Basic assertions
    assert result_gdf is not None
    assert not result_gdf.empty
    assert result_gdf.crs.to_epsg() == 4326  # type: ignore # Should be transformed

    # Check that we get one dissolved polygon for Completed and none for Action Required
    assert len(result_gdf[result_gdf["status_category"] == "Action Required"]) == 0
    assert len(result_gdf[result_gdf["status_category"] == "Completed"]) == 1  # Dissolved to 1


@patch("unified_pipeline.silver.bnbo_status.validate_and_transform_geometries")
def test_create_dissolved_df_empty_input(
    mock_validate_transform: MagicMock, bnbo_status_silver: BNBOStatusSilver
) -> None:
    """Test _create_dissolved_df with an empty GeoDataFrame."""
    # Create empty GeoDataFrame
    empty_gdf = gpd.GeoDataFrame({"status_category": [], "geometry": []}, crs="EPSG:4326")

    # Mock to return an empty GeoDataFrame with correct columns
    mock_validate_transform.side_effect = lambda gdf, _: gpd.GeoDataFrame(
        columns=["status_category", "geometry"], crs="EPSG:4326", geometry="geometry"
    )

    result_gdf = bnbo_status_silver._create_dissolved_df(empty_gdf.copy(), "test_dataset_empty")

    # Basic assertions
    assert result_gdf is not None
    assert result_gdf.empty  # Should be empty
    mock_validate_transform.assert_called_once()


@patch("unified_pipeline.silver.bnbo_status.validate_and_transform_geometries")
def test_create_dissolved_df_validate_transform_call(
    mock_validate_transform: MagicMock, bnbo_status_silver: BNBOStatusSilver
) -> None:
    """Test that validate_and_transform_geometries is called correctly."""
    # Create a simple GeoDataFrame
    data = {
        "status_category": ["Action Required"],
        "geometry": [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
    }
    input_gdf = gpd.GeoDataFrame(data, crs="EPSG:25832")
    mock_validate_transform.side_effect = lambda gdf, _: gdf

    # Call the method
    bnbo_status_silver._create_dissolved_df(input_gdf.copy(), "test_validate_call")

    # Check that the validation function was called with the right arguments
    mock_validate_transform.assert_called_once()
    called_gdf = mock_validate_transform.call_args[0][0]
    assert isinstance(called_gdf, gpd.GeoDataFrame)
    assert called_gdf.crs.to_epsg() == 4326  # type: ignore
    assert "status_category" in called_gdf.columns
    assert called_gdf["status_category"].iloc[0] == "Action Required"


def test_exception_handling_in_create_dissolved_df(
    bnbo_status_silver: BNBOStatusSilver,
) -> None:
    """Test exception handling in _create_dissolved_df."""
    # Create a GeoDataFrame with invalid geometries
    data = {
        "status_category": ["Action Required"],
        "geometry": [None],  # Invalid geometry
    }
    input_gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")

    with patch(
        "unified_pipeline.silver.bnbo_status.validate_and_transform_geometries",
        side_effect=Exception("Invalid geometry"),
    ):
        with pytest.raises(Exception) as excinfo:
            bnbo_status_silver._create_dissolved_df(input_gdf.copy(), "test_invalid_geom")
            assert "Invalid geometry" in str(excinfo.value)


def test_save_data(
    bnbo_status_silver: BNBOStatusSilver,
    mock_gcs_util: MagicMock,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test saving data to GCS."""
    # Create a sample GeoDataFrame
    data = {
        "status_category": ["Action Required"],
        "geometry": [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
    }
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")

    # Mock the GCS client and bucket
    mock_bucket = MagicMock()
    mock_gcs_util.get_gcs_client.return_value.bucket.return_value = mock_bucket
    current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    # Call the save_data method
    bnbo_status_silver._save_data(gdf, silver_config.dataset, silver_config.bucket)

    # Check that the blob was created and uploaded correctly
    mock_bucket.blob.assert_called_once_with(f"silver/bnbo_status/{current_date}.parquet")
    mock_bucket.blob().upload_from_filename.assert_called_once()


def test_save_data_with_empty_dataframe(
    bnbo_status_silver: BNBOStatusSilver,
    mock_gcs_util: MagicMock,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test saving an empty GeoDataFrame to GCS."""
    # Create an empty GeoDataFrame
    gdf = gpd.GeoDataFrame(columns=["status_category", "geometry"], crs="EPSG:4326")

    # Mock the GCS client and bucket
    mock_bucket = MagicMock()
    mock_gcs_util.get_gcs_client.return_value.bucket.return_value = mock_bucket

    # Call the save_data method
    bnbo_status_silver._save_data(gdf, silver_config.dataset, silver_config.bucket)

    # Check that the blob was not created
    mock_bucket.blob.assert_not_called()


@pytest.mark.asyncio
async def test_run(
    bnbo_status_silver: BNBOStatusSilver,
    silver_config: BNBOStatusSilverConfig,
) -> None:
    """Test the run method."""
    # Mock the read_data and save_data methods
    with (
        patch.object(
            bnbo_status_silver, "_read_bronze_data", return_value=pd.DataFrame()
        ) as mock_read_data,
        patch.object(
            bnbo_status_silver, "_process_xml_data", return_value=pd.DataFrame()
        ) as mock_process_xml_data,
        patch.object(
            bnbo_status_silver, "_create_dissolved_df", return_value=pd.DataFrame()
        ) as mock_create_dissolved_df,
        patch.object(bnbo_status_silver, "_save_data") as mock_save_data,
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
    """Test the run method with an empty DataFrame."""
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
            bnbo_status_silver, "_read_bronze_data", return_value=pd.DataFrame()
        ) as mock_read_data,
        patch.object(
            bnbo_status_silver, "_process_xml_data", return_value=None
        ) as mock_process_xml_data,
    ):
        await bnbo_status_silver.run()

        # Check that the methods were called
        mock_read_data.assert_called_once_with(silver_config.dataset, silver_config.bucket)
        mock_process_xml_data.assert_called_once()
