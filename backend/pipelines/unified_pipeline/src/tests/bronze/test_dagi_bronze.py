"""Tests for Datafordeler DAGI WFS bronze ingestion."""

import asyncio
from io import StringIO
from unittest.mock import AsyncMock, MagicMock

import pytest
from loguru import logger
from shapely.geometry import MultiPolygon, Point, Polygon, box

from unified_pipeline.bronze.dagi import DAGIBronze, DAGIBronzeConfig

GML_NS = "http://www.opengis.net/gml/3.2"
DAGI_NS = "http://data.gov.dk/schemas/dagi/2/gml3sfp/dagi10"
WFS_NS = "http://www.opengis.net/wfs/2.0"


def make_gml_feature(
    feature_type: str,
    attributes: dict[str, str],
    geometry: str,
) -> bytes:
    """Build a small WFS GML response using the Datafordeler element structure."""
    properties = "".join(
        f"<dagi10:{key}>{value}</dagi10:{key}>" for key, value in attributes.items()
    )
    return f"""<wfs:FeatureCollection xmlns:wfs="{WFS_NS}"
      xmlns:dagi10="{DAGI_NS}" xmlns:gml="{GML_NS}">
      <wfs:member><dagi10:{feature_type}>{properties}
        <dagi10:geometri>{geometry}</dagi10:geometri>
      </dagi10:{feature_type}></wfs:member>
    </wfs:FeatureCollection>""".encode()


def polygon_gml(
    exterior: str,
    interiors: tuple[str, ...] = (),
    *,
    element: str = "Polygon",
) -> str:
    holes = "".join(
        f"<gml:interior><gml:LinearRing><gml:posList>{ring}</gml:posList>"
        "</gml:LinearRing></gml:interior>"
        for ring in interiors
    )
    return (
        f"<gml:{element}><gml:exterior><gml:LinearRing>"
        f"<gml:posList>{exterior}</gml:posList></gml:LinearRing></gml:exterior>"
        f"{holes}</gml:{element}>"
    )


def bronze(monkeypatch: pytest.MonkeyPatch) -> DAGIBronze:
    monkeypatch.setenv("DATAFORDELER_API_KEY", "test-api-key")
    source = DAGIBronze.__new__(DAGIBronze)
    source.config = DAGIBronzeConfig()
    source.semaphore = asyncio.Semaphore(source.config.max_concurrent_requests)
    source.storage = MagicMock()
    source.log = logger
    return source


def test_parse_multisurface_keeps_polygons_and_holes(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    geometry_xml = (
        "<gml:MultiSurface>"
        "<gml:surfaceMember>"
        + polygon_gml(
            "0 0 10 0 10 10 0 10 0 0",
            ("2 2 2 4 4 4 4 2 2 2",),
        )
        + "</gml:surfaceMember><gml:surfaceMember>"
        + polygon_gml("20 0 25 0 25 5 20 5 20 0")
        + "</gml:surfaceMember></gml:MultiSurface>"
    )
    gml = make_gml_feature(
        "Kommuneinddeling",
        {"id.lokalId": "feature-1", "navn": "Testkommune"},
        geometry_xml,
    )

    record = source._parse_gml_page("kommuner", gml)[0]
    geometry = record["geometry_utm"]

    assert isinstance(geometry, MultiPolygon)
    assert geometry.is_valid
    assert len(geometry.geoms) == 2
    assert len(geometry.geoms[0].interiors) == 1
    assert (
        geometry.area
        == Polygon([(0, 0), (10, 0), (10, 10), (0, 10)], [[(2, 2), (2, 4), (4, 4), (4, 2)]]).area
        + box(20, 0, 25, 5).area
    )


def test_kommune_mapping_joins_region_and_normalizes_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = bronze(monkeypatch)
    kommuner = [
        {
            "attributes": {
                "id.lokalId": "k-1",
                "kommunekode": "101",
                "navn": "Testkommune",
                "regionskode": "1084",
                "udenforKommuneinddeling": "false",
            },
            "geometry_utm": box(723000, 6175000, 724000, 6176000),
        }
    ]
    regions = [
        {
            "attributes": {"id.lokalId": "r-1", "regionskode": "1084", "navn": "Region H"},
            "geometry_utm": box(722000, 6174000, 725000, 6177000),
        }
    ]

    collection = source._build_geojson("kommuner", kommuner, regions, "2026-10-10T00:00:00+00:00")
    properties = collection["features"][0]["properties"]

    assert properties["kode"] == "0101"
    assert properties["regionskode"] == "1084"
    assert properties["regionsnavn"] == "Region H"
    assert properties["udenforkommuneinddeling"] is False
    assert properties["dagi_id"] == "k-1"


def test_landsdel_is_assigned_to_containing_region(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    landsdele = [
        {
            "attributes": {"id.lokalId": "l-1", "NUTS3vaerdi": "DK011", "navn": "Byen København"},
            "geometry_utm": box(723000, 6175000, 724000, 6176000),
        }
    ]
    regions = [
        {
            "attributes": {"id.lokalId": "r-1", "regionskode": "1084", "navn": "Region H"},
            "geometry_utm": box(722000, 6174000, 725000, 6177000),
        }
    ]

    collection = source._build_geojson("landsdele", landsdele, regions, "2026-10-10T00:00:00+00:00")
    properties = collection["features"][0]["properties"]

    assert properties["nuts3"] == "DK011"
    assert properties["regionskode"] == "1084"
    assert properties["regionsnavn"] == "Region H"


def test_postnummer_mapping_keeps_code_as_string(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    records = [
        {
            "attributes": {
                "id.lokalId": "p-1",
                "postnummer": "1050",
                "navn": "København K",
                "erGadepostnummer": "true",
            },
            "geometry_utm": box(723000, 6175000, 724000, 6176000),
        }
    ]

    collection = source._build_geojson("postnumre", records, [], "2026-10-10T00:00:00+00:00")
    properties = collection["features"][0]["properties"]

    assert properties["nr"] == "1050"
    assert properties["ergadepostnummer"] is True
    assert properties["stormodtager"] is False


def test_wgs84_transform_matches_known_copenhagen_point(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)

    point = source._to_wgs84_geometry(Point(724434.93, 6175755.61))

    assert point.x == pytest.approx(12.5690, abs=0.001)
    assert point.y == pytest.approx(55.6757, abs=0.0001)


def make_page(member_count: int) -> bytes:
    members = "<wfs:member/>" * member_count
    return f'<wfs:FeatureCollection xmlns:wfs="{WFS_NS}">{members}</wfs:FeatureCollection>'.encode()


def mock_session_for_pages(pages: list[bytes]) -> MagicMock:
    session = MagicMock()
    responses = []
    for page in pages:
        response = AsyncMock()
        response.read.return_value = page
        response.raise_for_status = MagicMock()
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=None)
        responses.append(context)
    session.get.side_effect = responses
    return session


@pytest.mark.asyncio
async def test_paging_stops_after_short_page(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    source.config.page_sizes["kommuner"] = 2
    session = mock_session_for_pages([make_page(2), make_page(1)])

    pages = await source._fetch_layer_data(session, "kommuner", "Kommuneinddeling")

    assert pages == [make_page(2), make_page(1)]
    assert session.get.call_count == 2
    assert [call.kwargs["params"]["startIndex"] for call in session.get.call_args_list] == [
        "0",
        "2",
    ]


@pytest.mark.asyncio
async def test_api_key_is_sent_but_not_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    session = mock_session_for_pages([make_page(0)])
    output = StringIO()
    handler_id = logger.add(output, format="{message}")

    try:
        await source._fetch_page(session, "kommuner", "Kommuneinddeling", 0, 25)
    finally:
        logger.remove(handler_id)

    params = session.get.call_args.kwargs["params"]
    assert params["apiKey"] == "test-api-key"
    assert "test-api-key" not in output.getvalue()


def test_validation_rejects_duplicate_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)
    records = [
        {"attributes": {"regionskode": "1081"}},
        {"attributes": {"regionskode": "1081"}},
        {"attributes": {"regionskode": "1082"}},
        {"attributes": {"regionskode": "1083"}},
        {"attributes": {"regionskode": "1084"}},
    ]

    with pytest.raises(ValueError, match="duplicate"):
        source._validate_layer("regioner", records)


def test_validation_rejects_wrong_region_count(monkeypatch: pytest.MonkeyPatch) -> None:
    source = bronze(monkeypatch)

    with pytest.raises(ValueError, match="expected 5"):
        source._validate_layer(
            "regioner", [{"attributes": {"regionskode": str(i)}} for i in range(4)]
        )
