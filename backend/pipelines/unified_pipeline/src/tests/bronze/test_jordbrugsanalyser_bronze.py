"""Tests for Jordbrugsanalyser bronze compact GML parsing."""

from unified_pipeline.bronze.jordbrugsanalyser import (
    JordbrugsanalyserBronze,
    JordbrugsanalyserBronzeConfig,
)


def _synthetic_gml() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:Jordbrugsanalyser="Jordbrugsanalyser"
    numberMatched="2"
    numberReturned="2">
  <wfs:member>
    <Jordbrugsanalyser:Marker24 gml:id="Marker24.1">
      <Jordbrugsanalyser:the_geom>
        <gml:MultiSurface srsName="EPSG:25832">
          <gml:surfaceMember>
            <gml:Polygon>
              <gml:exterior>
                <gml:LinearRing>
                  <gml:posList>500000 6100000 500010 6100000 500010 6100010 500000 6100010 500000 6100000</gml:posList>
                </gml:LinearRing>
              </gml:exterior>
            </gml:Polygon>
          </gml:surfaceMember>
        </gml:MultiSurface>
      </Jordbrugsanalyser:the_geom>
      <Jordbrugsanalyser:EjerNr>12345678</Jordbrugsanalyser:EjerNr>
      <Jordbrugsanalyser:MarkBlok>11-22</Jordbrugsanalyser:MarkBlok>
      <Jordbrugsanalyser:MarkNr>7</Jordbrugsanalyser:MarkNr>
      <Jordbrugsanalyser:AfgKat>Korn</Jordbrugsanalyser:AfgKat>
      <Jordbrugsanalyser:AfgNavn>Vinterhvede</Jordbrugsanalyser:AfgNavn>
      <Jordbrugsanalyser:AfgNr>10</Jordbrugsanalyser:AfgNr>
      <Jordbrugsanalyser:Ha>1.25</Jordbrugsanalyser:Ha>
      <Jordbrugsanalyser:HaIalt>1.25</Jordbrugsanalyser:HaIalt>
      <Jordbrugsanalyser:X>500005</Jordbrugsanalyser:X>
      <Jordbrugsanalyser:Y>6100005</Jordbrugsanalyser:Y>
    </Jordbrugsanalyser:Marker24>
  </wfs:member>
  <wfs:member>
    <Jordbrugsanalyser:Marker24 gml:id="Marker24.2">
      <Jordbrugsanalyser:the_geom>
        <gml:MultiSurface srsName="EPSG:25832">
          <gml:surfaceMember>
            <gml:Polygon>
              <gml:exterior>
                <gml:LinearRing>
                  <gml:posList>500020 6100020 500030 6100020 500030 6100030 500020 6100030 500020 6100020</gml:posList>
                </gml:LinearRing>
              </gml:exterior>
            </gml:Polygon>
          </gml:surfaceMember>
        </gml:MultiSurface>
      </Jordbrugsanalyser:the_geom>
      <Jordbrugsanalyser:EjerNr>87654321</Jordbrugsanalyser:EjerNr>
      <Jordbrugsanalyser:MarkBlok>33-44</Jordbrugsanalyser:MarkBlok>
      <Jordbrugsanalyser:MarkNr>8</Jordbrugsanalyser:MarkNr>
      <Jordbrugsanalyser:AfgKat>Græs</Jordbrugsanalyser:AfgKat>
      <Jordbrugsanalyser:AfgNavn>Permanent græs</Jordbrugsanalyser:AfgNavn>
      <Jordbrugsanalyser:AfgNr>250</Jordbrugsanalyser:AfgNr>
      <Jordbrugsanalyser:Ha>2.5</Jordbrugsanalyser:Ha>
      <Jordbrugsanalyser:HaIalt>2.5</Jordbrugsanalyser:HaIalt>
      <Jordbrugsanalyser:X>500025</Jordbrugsanalyser:X>
      <Jordbrugsanalyser:Y>6100025</Jordbrugsanalyser:Y>
    </Jordbrugsanalyser:Marker24>
  </wfs:member>
</wfs:FeatureCollection>
"""


def test_jordbrugsanalyser_bronze_parses_gml_to_compact_structured_table() -> None:
    bronze = JordbrugsanalyserBronze(JordbrugsanalyserBronzeConfig(save_local=True))

    rows = bronze._parse_wfs_response(_synthetic_gml(), 2024)
    assert len(rows) == 2
    assert rows[0]["owner_number"] == 12345678
    assert rows[0]["field_block"] == "11-22"
    assert rows[0]["field_number"] == "7"
    assert rows[0]["crop_name"] == "Vinterhvede"
    assert rows[0]["geometry_wkt"].startswith("POLYGON")

    table_name = bronze._create_structured_bronze_table([_synthetic_gml()], 2024)
    assert table_name is not None

    result = bronze.conn.execute(f"""
        SELECT
            owner_number,
            field_block,
            field_number,
            crop_code,
            year,
            TRY(ST_IsValid(geometry)) AS is_valid_geometry,
            ST_AsText(geometry) AS geometry_wkt
        FROM {table_name}
        ORDER BY owner_number
    """).fetchall()

    assert result == [
        (12345678, "11-22", "7", 10, 2024, True, result[0][6]),
        (87654321, "33-44", "8", 250, 2024, True, result[1][6]),
    ]
    assert all(row[6].startswith("POLYGON") for row in result)
