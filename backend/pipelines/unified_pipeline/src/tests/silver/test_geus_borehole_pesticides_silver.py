"""
Tests for the GEUSBoreholePesticidesSilver class.
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

import pytest

from unified_pipeline.silver.geus_borehole_pesticides import (
    GEUSBoreholePesticidesSilver,
    GEUSBoreholePesticidesSilverConfig,
)


@pytest.fixture
def config() -> GEUSBoreholePesticidesSilverConfig:
    """Return a test configuration."""
    return GEUSBoreholePesticidesSilverConfig(
        dataset="test_geus_borehole_pesticides",
        bucket="test-bucket",
        storage_batch_size=1000,
    )


@pytest.fixture
def silver_source(config: GEUSBoreholePesticidesSilverConfig) -> GEUSBoreholePesticidesSilver:
    """Return a test GEUSBoreholePesticidesSilver instance."""
    source = GEUSBoreholePesticidesSilver(config)
    source.log = MagicMock()
    return source


@pytest.fixture
def sample_borehole_gml() -> str:
    """Return a sample GML string for boreholes."""
    # GEUS uses MapServer namespace (ms:) for features
    return """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
                          xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                          xmlns:gml="http://www.opengis.net/gml/3.2"
                          numberMatched="2" numberReturned="2">
        <wfs:member>
            <ms:jupiter_boringer_ws gml:id="borehole1">
                <ms:dgunr>12345678</ms:dgunr>
                <ms:anlaegid>A001</ms:anlaegid>
                <ms:komnr>101</ms:komnr>
                <ms:kommunenavn>Copenhagen</ms:kommunenavn>
                <ms:region_tekst>Hovedstaden</ms:region_tekst>
                <ms:dybde>50m</ms:dybde>
                <ms:formaal_tekst>Vandforsyning</ms:formaal_tekst>
                <ms:kode_tekst>Aktiv</ms:kode_tekst>
                <ms:hovedtype>Boring</ms:hovedtype>
                <gml:Point>
                    <gml:pos>725000 6175000</gml:pos>
                </gml:Point>
            </ms:jupiter_boringer_ws>
        </wfs:member>
        <wfs:member>
            <ms:jupiter_boringer_ws gml:id="borehole2">
                <ms:dgunr>87654321</ms:dgunr>
                <ms:anlaegid>A002</ms:anlaegid>
                <ms:komnr>147</ms:komnr>
                <ms:kommunenavn>Frederiksberg</ms:kommunenavn>
                <ms:region_tekst>Hovedstaden</ms:region_tekst>
                <ms:dybde>30m</ms:dybde>
                <ms:formaal_tekst>Grundvandsovervågning</ms:formaal_tekst>
                <ms:kode_tekst>Aktiv</ms:kode_tekst>
                <ms:hovedtype>Boring</ms:hovedtype>
                <gml:Point>
                    <gml:pos>726000 6176000</gml:pos>
                </gml:Point>
            </ms:jupiter_boringer_ws>
        </wfs:member>
    </wfs:FeatureCollection>
    """


@pytest.fixture
def sample_analyses_gml() -> str:
    """Return a sample GML string for analyses with tracked pesticides."""
    # GEUS uses MapServer namespace (ms:) and _num suffix for numeric fields
    return """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
                          xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                          xmlns:gml="http://www.opengis.net/gml/3.2"
                          numberMatched="3" numberReturned="3">
        <wfs:member>
            <ms:jupiter_anlaegsanalyser gml:id="analysis1">
                <ms:anlaegid_num>A001</ms:anlaegid_num>
                <ms:stofnr_num>438</ms:stofnr_num>
                <ms:stof>2,6-Dichlorbenzamid (BAM)</ms:stof>
                <ms:stof_status>Aktiv</ms:stof_status>
                <ms:maengde_num>0.15</ms:maengde_num>
                <ms:enhed>ug/l</ms:enhed>
                <ms:proevedato>2024-01-15</ms:proevedato>
                <ms:xutm32euref89>725000</ms:xutm32euref89>
                <ms:yutm32euref89>6175000</ms:yutm32euref89>
            </ms:jupiter_anlaegsanalyser>
        </wfs:member>
        <wfs:member>
            <ms:jupiter_anlaegsanalyser gml:id="analysis2">
                <ms:anlaegid_num>A001</ms:anlaegid_num>
                <ms:stofnr_num>846</ms:stofnr_num>
                <ms:stof>Atrazin</ms:stof>
                <ms:stof_status>Aktiv</ms:stof_status>
                <ms:maengde_num>0.05</ms:maengde_num>
                <ms:enhed>ug/l</ms:enhed>
                <ms:proevedato>2024-01-15</ms:proevedato>
                <ms:xutm32euref89>725000</ms:xutm32euref89>
                <ms:yutm32euref89>6175000</ms:yutm32euref89>
            </ms:jupiter_anlaegsanalyser>
        </wfs:member>
        <wfs:member>
            <ms:jupiter_anlaegsanalyser gml:id="analysis3">
                <ms:anlaegid_num>A002</ms:anlaegid_num>
                <ms:stofnr_num>9999</ms:stofnr_num>
                <ms:stof>Non-tracked substance</ms:stof>
                <ms:maengde_num>1.0</ms:maengde_num>
                <ms:enhed>ug/l</ms:enhed>
                <ms:proevedato>2024-01-16</ms:proevedato>
            </ms:jupiter_anlaegsanalyser>
        </wfs:member>
    </wfs:FeatureCollection>
    """


def test_config_defaults() -> None:
    """Test default configuration values."""
    config = GEUSBoreholePesticidesSilverConfig()

    assert config.dataset == "geus_borehole_pesticides"
    assert config.bucket == "landbrugsdata-raw-data"
    assert config.source_crs == "EPSG:25832"
    assert 438 in config.tracked_pesticides  # BAM
    assert 846 in config.tracked_pesticides  # Atrazin
    assert 613 in config.tracked_pesticides  # Chloridazon
    assert 1448 in config.tracked_pesticides  # Desphenyl chloridazon
    assert 1534 in config.tracked_pesticides  # Methyl-desphenyl-chloridazon


def test_get_element_text(silver_source: GEUSBoreholePesticidesSilver) -> None:
    """Test getting element text from XML."""
    # GEUS uses MapServer namespace (ms:) not custom namespace
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <feature xmlns:ms="http://mapserver.gis.umn.edu/mapserver">
        <ms:dgunr>12345678</ms:dgunr>
        <ms:empty></ms:empty>
    </feature>
    """
    root = ET.fromstring(xml_str)

    dgunr = silver_source._get_element_text(root, "ms:dgunr")
    empty = silver_source._get_element_text(root, "ms:empty")
    missing = silver_source._get_element_text(root, "ms:missing")

    assert dgunr == "12345678"
    assert empty is None  # Empty element returns None
    assert missing is None


def test_parse_borehole_feature_valid(silver_source: GEUSBoreholePesticidesSilver) -> None:
    """Test parsing a valid borehole feature."""
    # GEUS uses MapServer namespace (ms:) for features
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_boringer_ws xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                            xmlns:gml="http://www.opengis.net/gml/3.2"
                            gml:id="borehole1">
        <ms:dgunr>12345678</ms:dgunr>
        <ms:anlaegid>A001</ms:anlaegid>
        <ms:komnr>101</ms:komnr>
        <ms:kommunenavn>Copenhagen</ms:kommunenavn>
        <ms:region_tekst>Hovedstaden</ms:region_tekst>
        <ms:dybde>50m</ms:dybde>
        <ms:formaal_tekst>Vandforsyning</ms:formaal_tekst>
        <ms:kode_tekst>Aktiv</ms:kode_tekst>
        <ms:hovedtype>Boring</ms:hovedtype>
        <gml:Point>
            <gml:pos>725000 6175000</gml:pos>
        </gml:Point>
    </ms:jupiter_boringer_ws>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_borehole_feature(root)

    assert result is not None
    assert result["dgunr"] == "12345678"
    assert result["anlaegid"] == "A001"
    assert result["x"] == 725000.0
    assert result["y"] == 6175000.0
    assert result["kommunenavn"] == "Copenhagen"
    assert result["region_tekst"] == "Hovedstaden"
    assert result["dybde"] == "50m"


def test_parse_borehole_feature_missing_dgunr(silver_source: GEUSBoreholePesticidesSilver) -> None:
    """Test parsing a borehole feature without dgunr."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_boringer_ws xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                            xmlns:gml="http://www.opengis.net/gml/3.2">
        <ms:anlaegid>A001</ms:anlaegid>
        <gml:Point>
            <gml:pos>725000 6175000</gml:pos>
        </gml:Point>
    </ms:jupiter_boringer_ws>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_borehole_feature(root)

    assert result is None


def test_parse_borehole_feature_missing_point(silver_source: GEUSBoreholePesticidesSilver) -> None:
    """Test parsing a borehole feature without coordinates."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_boringer_ws xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                            xmlns:gml="http://www.opengis.net/gml/3.2">
        <ms:dgunr>12345678</ms:dgunr>
        <ms:anlaegid>A001</ms:anlaegid>
    </ms:jupiter_boringer_ws>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_borehole_feature(root)

    assert result is None


def test_parse_analysis_feature_tracked_pesticide(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test parsing an analysis feature with a tracked pesticide."""
    # GEUS uses MapServer namespace (ms:) and _num suffix for numeric fields
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_anlaegsanalyser xmlns:ms="http://mapserver.gis.umn.edu/mapserver">
        <ms:anlaegid_num>A001</ms:anlaegid_num>
        <ms:stofnr_num>438</ms:stofnr_num>
        <ms:stof>2,6-Dichlorbenzamid (BAM)</ms:stof>
        <ms:stof_status>Aktiv</ms:stof_status>
        <ms:maengde_num>0.15</ms:maengde_num>
        <ms:enhed>ug/l</ms:enhed>
        <ms:proevedato>2024-01-15</ms:proevedato>
        <ms:xutm32euref89>725000</ms:xutm32euref89>
        <ms:yutm32euref89>6175000</ms:yutm32euref89>
    </ms:jupiter_anlaegsanalyser>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_analysis_feature(root)

    assert result is not None
    assert result["anlaegid"] == "A001"
    assert result["stofnr"] == 438
    assert result["stof"] == "2,6-Dichlorbenzamid (BAM)"
    assert result["maengde"] == 0.15
    assert result["enhed"] == "ug/l"
    assert result["data_source"] == "jupiter_anlaegsanalyser"


def test_parse_analysis_feature_non_tracked_pesticide(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test parsing an analysis feature with a non-tracked substance."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_anlaegsanalyser xmlns:ms="http://mapserver.gis.umn.edu/mapserver">
        <ms:anlaegid_num>A001</ms:anlaegid_num>
        <ms:stofnr_num>9999</ms:stofnr_num>
        <ms:stof>Some other substance</ms:stof>
        <ms:maengde_num>1.0</ms:maengde_num>
    </ms:jupiter_anlaegsanalyser>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_analysis_feature(root)

    assert result is None  # Non-tracked pesticide should be filtered out


def test_parse_analysis_feature_missing_stofnr(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test parsing an analysis feature without stofnr."""
    xml_str = """<?xml version="1.0" encoding="UTF-8"?>
    <ms:jupiter_anlaegsanalyser xmlns:ms="http://mapserver.gis.umn.edu/mapserver">
        <ms:anlaegid_num>A001</ms:anlaegid_num>
        <ms:stof>Some substance</ms:stof>
    </ms:jupiter_anlaegsanalyser>
    """
    root = ET.fromstring(xml_str)

    result = silver_source._parse_analysis_feature(root)

    assert result is None


def test_parse_boreholes_gml(
    silver_source: GEUSBoreholePesticidesSilver, sample_borehole_gml: str
) -> None:
    """Test parsing boreholes from GML data."""
    result = silver_source._parse_boreholes_gml([sample_borehole_gml])

    assert len(result) == 2
    assert result[0]["dgunr"] == "12345678"
    assert result[1]["dgunr"] == "87654321"


def test_parse_analyses_gml_filters_non_tracked(
    silver_source: GEUSBoreholePesticidesSilver, sample_analyses_gml: str
) -> None:
    """Test that parsing analyses GML filters out non-tracked pesticides."""
    result = silver_source._parse_analyses_gml([sample_analyses_gml])

    # Should only return 2 analyses (BAM and Atrazin), not the non-tracked one
    assert len(result) == 2
    stofnr_values = {a["stofnr"] for a in result}
    assert 438 in stofnr_values  # BAM
    assert 846 in stofnr_values  # Atrazin
    assert 9999 not in stofnr_values  # Non-tracked


def test_create_duckdb_tables(
    silver_source: GEUSBoreholePesticidesSilver,
    sample_borehole_gml: str,
    sample_analyses_gml: str,
) -> None:
    """Test creating DuckDB tables from parsed data."""
    boreholes = silver_source._parse_boreholes_gml([sample_borehole_gml])
    analyses = silver_source._parse_analyses_gml([sample_analyses_gml])

    boreholes_table, analyses_table, joined_table = silver_source._create_duckdb_tables(
        boreholes, analyses
    )

    # Verify boreholes table
    assert boreholes_table == "geus_boreholes"
    bh_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {boreholes_table}").fetchone()[0]
    assert bh_count == 2

    # Verify analyses table
    assert analyses_table == "geus_pesticide_analyses"
    an_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {analyses_table}").fetchone()[0]
    assert an_count == 2

    # Verify joined table - only A001 has pesticide analyses
    assert joined_table == "geus_borehole_pesticides_joined"
    joined_count = silver_source.conn.execute(f"SELECT COUNT(*) FROM {joined_table}").fetchone()[0]
    assert joined_count == 2  # Both analyses linked to A001


def test_validate_geometries_within_bounds(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test geometry validation within Denmark bounds."""
    # Create test table with valid coordinates
    silver_source.conn.execute("""
        CREATE TABLE test_valid_geom AS
        SELECT ST_Point(725000, 6175000) as geometry
        UNION ALL
        SELECT ST_Point(726000, 6176000) as geometry
    """)

    # Should not raise or log warnings
    silver_source._validate_geometries("test_valid_geom")

    # Verify log was called with success message
    assert any("validated" in str(call).lower() for call in silver_source.log.info.call_args_list)


def test_validate_geometries_outside_bounds(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test geometry validation with points outside Denmark bounds."""
    # Create test table with one valid and one invalid coordinate
    silver_source.conn.execute("""
        CREATE TABLE test_invalid_geom AS
        SELECT ST_Point(725000, 6175000) as geometry
        UNION ALL
        SELECT ST_Point(0, 0) as geometry
    """)

    # Should log a warning
    silver_source._validate_geometries("test_invalid_geom")

    # Verify warning was logged
    assert silver_source.log.warning.called


@pytest.mark.asyncio
async def test_run_success_with_bronze_data(
    silver_source: GEUSBoreholePesticidesSilver,
    sample_borehole_gml: str,
    sample_analyses_gml: str,
) -> None:
    """Test successful run with in-memory bronze data."""
    bronze_data = {
        "boreholes": [sample_borehole_gml],
        "analyses": [sample_analyses_gml],
        "pesticide_analyses": [],  # Empty mc_analyse data for this test
    }

    silver_source.save_data_direct = MagicMock()

    result = await silver_source.run(bronze_data=bronze_data)

    assert result is not None
    assert result["status"] == "completed"
    assert result["dataset"] == "test_geus_borehole_pesticides"
    assert silver_source.save_data_direct.call_count >= 1


@pytest.mark.asyncio
async def test_run_no_boreholes_parsed(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test run with invalid GML data that produces no boreholes."""
    bronze_data = {
        "boreholes": ["<invalid>not valid GML</invalid>"],
        "analyses": ["<invalid>not valid GML</invalid>"],
        "pesticide_analyses": [],
    }

    result = await silver_source.run(bronze_data=bronze_data)

    assert result is None


@pytest.mark.asyncio
async def test_run_with_invalid_bronze_data_type(
    silver_source: GEUSBoreholePesticidesSilver,
) -> None:
    """Test run with invalid bronze data type."""
    bronze_data = "not a dict"

    result = await silver_source.run(bronze_data=bronze_data)

    assert result is None


@pytest.mark.asyncio
async def test_run_no_pesticide_analyses(
    silver_source: GEUSBoreholePesticidesSilver,
    sample_borehole_gml: str,
) -> None:
    """Test run when no pesticide analyses are found."""
    # Only non-tracked substance in analyses (using MapServer namespace)
    analyses_gml = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
                          xmlns:ms="http://mapserver.gis.umn.edu/mapserver"
                          numberMatched="1" numberReturned="1">
        <wfs:member>
            <ms:jupiter_anlaegsanalyser>
                <ms:anlaegid_num>A001</ms:anlaegid_num>
                <ms:stofnr_num>9999</ms:stofnr_num>
                <ms:stof>Non-tracked substance</ms:stof>
                <ms:maengde_num>1.0</ms:maengde_num>
            </ms:jupiter_anlaegsanalyser>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    bronze_data = {
        "boreholes": [sample_borehole_gml],
        "analyses": [analyses_gml],
        "pesticide_analyses": [],  # Empty mc_analyse data
    }

    silver_source.save_data_direct = MagicMock()

    result = await silver_source.run(bronze_data=bronze_data)

    # Should still succeed, but with warning about no pesticide analyses
    assert result is not None
    assert result["status"] == "completed"
    assert silver_source.log.warning.called
